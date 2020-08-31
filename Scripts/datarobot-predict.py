"""
Usage for forecast predictions:
    python datarobot-predict.py \
        --forecast_point <date> <input-file.csv> <output-file.csv>

Usage for historical predictions:
    python datarobot-predict.py \
      --predictions_start_date <date> --predictions_end_date <date> \
      <input-file.csv> <output-file.csv>

We highly recommend that you update SSL certificates with:
    pip install -U urllib3[secure] certifi

Details: https://app.datarobot.com/docs/predictions/batch/batch-prediction-api/index.html
"""
import argparse
import contextlib
import json
import logging
import os
import sys
import time
import threading

try:
    from urllib2 import urlopen, HTTPError, Request
except ImportError:
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError


API_KEY = 'NWVlMGIyMjcwYjU5ZjkxNDFjYjllZjlkOlZrVFY2ZXl0UUVpSklmYjhQbjFVZmcvaHB1ZG9xVmszWTlmVkMxY2R0Q2c9'
BATCH_PREDICTIONS_URL = 'https://app.datarobot.com/api/v2/batchPredictions/'
DEPLOYMENT_ID = '5f4c9c81bb594b1085d16908'
POLL_INTERVAL = 15
CHUNK = 64 * 1024

logs_counter = 0


logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, usage='python %(prog)s <input-file.csv> <output-file.csv>'
    )
    parser.add_argument('--forecast_point', help='The forecast point, use "infer" to infer from dataset')
    parser.add_argument('--predictions_start_date', help='Start date for historical predictions')
    parser.add_argument('--predictions_end_date', help='End date for historical predictions')
    parser.add_argument(
        'input_file', type=argparse.FileType('rb'), help='Input CSV file with data to be scored.'
    )
    parser.add_argument(
        'output_file', type=argparse.FileType('wb'), help='Output CSV file with the scored data.'
    )

    return parser.parse_args()


def main():
    args = parse_args()

    input_file = args.input_file
    output_file = args.output_file

    payload = {
        'deploymentId': DEPLOYMENT_ID,
        'includePredictionStatus': True,
    }
    if args.forecast_point:

        payload['timeseriesSettings'] = {'type': 'forecast'}
        if args.forecast_point != 'infer':
            payload['timeseriesSettings']['forecastPoint'] = args.forecast_point

    elif args.predictions_start_date and args.predictions_end_date:

        payload['timeseriesSettings'] = {
            'type': 'historical',
            'predictionsStartDate': args.predictions_start_date,
            'predictionsEndDate': args.predictions_end_date,
        }

    if 'timeseriesSettings' not in payload:
        logger.error(
            'You must pass either --forecast_point or '
            '--predictions_start_date and --predictions_end_date'
        )
        return 1

    try:
        make_datarobot_batch_predictions(input_file, output_file, payload)
    except DataRobotPredictionError as err:
        logger.error('Error: %s', err)
        return 1

    return 0


def make_datarobot_batch_predictions(input_file, output_file, payload):
    # Create new job for batch predictions
    job = _request('POST', BATCH_PREDICTIONS_URL, data=payload)
    links = job['links']

    logger.info(
        'Created Batch Prediction job ID {job_id} for deployment ID {deployment_id}'
        ' ({intake} -> {output}) on {csv_upload_url}.'.format(
            job_id=job['id'],
            deployment_id=DEPLOYMENT_ID,
            intake=job['jobSpec']['intakeSettings']['type'],
            output=job['jobSpec']['outputSettings']['type'],
            csv_upload_url=links['csvUpload'],
        )
    )

    # Simultaneously upload
    upload_stream = threading.Thread(
        target=upload_datarobot_batch_predictions, args=(job, input_file)
    )
    upload_stream.start()

    # Simultaneously download
    download_stream = threading.Thread(
        target=download_datarobot_batch_predictions, args=(job, output_file)
    )
    download_stream.start()

    # Wait until job's complete
    job_url = links['self']
    while True:
        try:
            job = _request('GET', job_url)
            status = job['status']
            if status == JobStatus.INITIALIZING:
                queue_position = job["queuePosition"]

                if queue_position > 0:
                    logger.info(
                        "Waiting for other jobs to complete: {}".format(queue_position)
                    )
                else:
                    logger.debug("No queuePosition yet. Waiting for one..")

                _check_logs(job)
                time.sleep(POLL_INTERVAL)
                continue

            elif status == JobStatus.RUNNING:
                logger.info(
                    'Waiting for the job to complete: {}%'.format(job['percentageCompleted'])
                )

                logger.info('Number of scored rows: {}'.format(job['scoredRows']))
                logger.info('Number of failed rows: {}'.format(job['failedRows']))
                logger.info('Number of skipped rows: {}'.format(job['skippedRows']))

                _check_logs(job)
                time.sleep(POLL_INTERVAL)
                continue

            elif status == JobStatus.COMPLETED:
                upload_stream.join()
                download_stream.join()

            _check_logs(job)
            return
        except Exception as e:
            if 'status' in job and not isinstance(e, DataRobotPredictionError):
                logger.exception('Unexpected error occurred')
                raise DataRobotPredictionError(
                    'An unexpected error occurred.\n\n'
                    '{err_type}: {err_msg}\n\n'
                    'Job {job_id} is {job_status}\n'
                    '{job_details}.\nLog: {job_logs}'.format(
                        err_type=type(e).__name__,
                        err_msg=e,
                        job_id=job['id'],
                        job_status=job['status'],
                        job_details=job['statusDetails'],
                        job_logs=job['logs'],
                    )
                )

            else:
                raise e


def upload_datarobot_batch_predictions(job_spec, input_file):
    logger.info('Start uploading csv data')

    upload_url = job_spec['links']['csvUpload']
    headers = {
        'Content-length': os.path.getsize(input_file.name),
        'Content-type': 'text/csv; encoding=utf-8',
    }
    try:
        response = _request('PUT', upload_url, data=input_file, headers=headers, to_json=False)
    except DataRobotPredictionError as err:
        logger.error('Error: %s', err)

    logger.info('Uploading is finished')


def download_datarobot_batch_predictions(job_spec, output_file):
    logger.info('Start downloading csv data')

    job_url = job_spec['links']['self']
    job_status = job_spec['status']

    while job_status not in JobStatus.DOWNLOADABLE:
        job_spec = _request('GET', job_url)
        job_status = job_spec['status']
        time.sleep(1)

    download_url = job_spec['links']['download']
    try:
        with contextlib.closing(_request('GET', download_url, to_json=False)) as response:
            while True:
                chunk = response.read(CHUNK)
                if not chunk:
                    break
                output_file.write(chunk)
    except DataRobotPredictionError as err:
        logger.error('Error: %s', err)

    logger.info('Results downloaded to: {}'.format(output_file.name))


def _request(method, url, data=None, headers=None, to_json=True):
    headers = _prepare_headers(headers)

    if isinstance(data, dict):
        data = json.dumps(data).encode('utf-8')  # for python3
        headers.update({'Content-Type': 'application/json; encoding=utf-8'})

    request = Request(url, headers=headers, data=data)
    request.get_method = lambda: method
    try:
        response = urlopen(request)
        if to_json:
            result = response.read()
            response.close()

            # json.loads() in 2.7 and prior to 3.6 needed strings, not bytes:
            # https://docs.python.org/3/whatsnew/3.6.html#json.
            if sys.version_info <= (3, 5):
                result = result.decode('utf-8')

            return json.loads(result)

        return response
    except HTTPError as e:
        err_msg = '{code} Error: {msg}'.format(code=e.code, msg=e.read())
        raise DataRobotPredictionError(err_msg)


def _prepare_headers(headers=None):
    if not headers:
        headers = {}
    headers.update({'Authorization': 'Bearer {}'.format(API_KEY)})
    return headers


def _check_logs(job):
    global logs_counter
    logs = job['logs']
    if len(logs) > logs_counter:
        new_logs = logs[logs_counter:]
        for log in new_logs:
            logger.info(log)
            logs_counter += 1


class DataRobotPredictionError(Exception):
    """Raised if there are issues getting predictions from DataRobot"""


class JobStatus(object):
    INITIALIZING = 'INITIALIZING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    ABORTED = 'ABORTED'

    DOWNLOADABLE = [RUNNING, COMPLETED, ABORTED]


if __name__ == '__main__':
    if sys.argv == None or (len(sys.argv) ==1 and len(sys.argv[0])==0):
        sys.argv=['dr_predict.py','--forecast_point','2020']
    sys.exit(main())
