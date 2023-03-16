import io
import json
import math
import os
from datetime import datetime, timedelta
from typing import Union

import boto3
import google.auth
import pandas as pd
from PIL import Image
from google.api_core.client_options import ClientOptions
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import vision
from jinja2 import Template
from pymysql import connect
from pymysql.cursors import DictCursor
from pdf2image import convert_from_path

from lib.namespace import RecursiveNamespace as Namespace


def get_stockholderfile_rows(db: Namespace) -> Union[list, tuple]:
    '''새로 추가된 주주명부 파일 정보를 gowid.StockholderFile 테이블에서 가져온다.'''
    print('get_stockholderfile_rows')
    conn = connect(
        host=db.host, port=db.port, db=db.schema, user=db.username, password=db.password,
        charset='utf8', cursorclass=DictCursor
    )
    with conn:
        with conn.cursor() as cur:
            execution_date = datetime.strptime(_execution_date(), '%Y-%m-%dT%H:%M:%S')
            start_date = execution_date.strftime('%Y-%m-%d')
            end_date = (execution_date + timedelta(days=1)).strftime('%Y-%m-%d')
            params = {'from': start_date, 'to': end_date}
            script = _get_script('sql/stockholderfile_rows.sql', params)
            cur.execute(script)
            rows = cur.fetchall()
            print('조회결과: {} rows'.format(len(rows)))
            for row in rows:
                print('조회결과: {}'.format(row))
            return rows


def detect_text(client: vision.ImageAnnotatorClient, convert_file: str) -> str:
    '''파일의 글자를 감지한다.'''
    with io.open(convert_file, 'rb') as image_file:
        content = image_file.read()
    image = vision.Image(content=content)
    response = client.text_detection(image=image)

    if response.error.message:
        raise Exception('OCR API ERROR: {}'.format(response.error.message))

    annotations = response.text_annotations
    if len(annotations) > 0:
        return annotations[0].description
    else:
        return ''


def run_ocr(client: vision.ImageAnnotatorClient, corporate_registration_number: str,
            convert_file: str) -> dict:
    '''ocr 결과를 stockholderfile 테이블 데이터와 합쳐서 dict로 내보낸다.'''
    text = detect_text(client, convert_file)
    base_date = datetime.strptime(_execution_date(), '%Y-%m-%dT%H:%M:%S') \
        .replace(hour=0, minute=0, second=0, microsecond=0)

    return {
        'reference_date': base_date,
        'corporate_registration_number': corporate_registration_number,
        'ocr_text': text
    }


def get_ocr_data(stockholderfile_rows: list, action_project: str) -> pd.DataFrame:
    '''google vision api 조회 결과를 bigquery 테이블에 적재한다.'''
    vision_client = vision.ImageAnnotatorClient(
        client_options=ClientOptions(quota_project_id=action_project))

    data = []
    for row in stockholderfile_rows:
        corporate_registration_number = row['fname'][:10]
        for convert_file in row['convert_files']:
            data.append(
                run_ocr(vision_client, corporate_registration_number, convert_file))

    return pd.DataFrame(data)


def upload_ocr_data_to_checkpoint_table(df: pd.DataFrame, checkpoint: Namespace,
                                        action_project: str) -> None:
    '''ocr_data를 checkpoint테이블에 로드한다.'''
    if len(df) == 0:
        return

    df_checkpoint = df \
        .groupby(['reference_date', 'corporate_registration_number'], as_index=False) \
        .agg({'ocr_text': lambda x: '\n'.join(x).split('\n')})

    project = getattr(checkpoint.project, _env())
    checkpoint_table_id = '{}.{}.{}'.format(project, checkpoint.dataset, checkpoint.name)
    base_date = datetime.strptime(_execution_date(), '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')

    client = bigquery.Client(project=action_project)
    params = {'project': project, 'reference_date': base_date}
    script = _get_script('sql/delete_checkpoint.sql', params)
    query_job = client.query(script)
    query_job.result()

    job_config = bigquery.LoadJobConfig(
        schema=[s.__dict__ for s in checkpoint.schema],
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields=checkpoint.clustering_fields,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field='reference_date',
            expiration_ms=315569260000
        )
    )
    load_job = client.load_table_from_dataframe(
        df_checkpoint, destination=checkpoint_table_id, job_config=job_config
    )

    job_result = load_job.result()
    job_count = job_result.output_rows

    if (data_count := len(df_checkpoint)) != job_count:
        raise Exception(
            '수집한 데이터와 테이블에 업로드된 데이터의 크기가 다릅니다: {} - {} != {}'
            .format(checkpoint_table_id, data_count, job_count)
        )

    print('빅쿼리 테이블 업로드 완료: {} - {} rows'.format(checkpoint_table_id, job_count))


def create_dir(paths: list) -> None:
    '''디렉토리를 생성한다.'''
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)


def copy_and_convert_files(stockholderfile_rows: list, bucket: str, s3_stockholder: Namespace,
                           action_project: str) -> list:
    '''s3에서 파일을 복사해서 gcs에 업로드하고, jpg, pdf파일을 convert 디렉토리로 이동'''
    print('copy_and_convert_files')

    storage_client = storage.Client(project=action_project)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=s3_stockholder.aws_access_key_id,
        aws_secret_access_key=s3_stockholder.aws_secret_access_key,
        region_name=s3_stockholder.region_name
    )

    s3_bucket = s3_stockholder.bucket

    origin_dir = '/origin'
    convert_dir = '/convert'
    create_dir([origin_dir, convert_dir])

    files = []
    for row in stockholderfile_rows:
        key = row['s3Key']
        fname = row['fname']
        origin_file = '{}/{}'.format(origin_dir, fname)

        download_file_from_s3(s3_client, s3_bucket, key, origin_file)
        upload_file_to_storage(storage_client, bucket, key, origin_file)

        if origin_file.upper().endswith(('JPG', 'JPEG', 'PNG')):
            convert_file = '{}/{}'.format(convert_dir, fname)
            resize_image(origin_file, convert_file)
            row['convert_files'] = [convert_file]
            files.append(row)
        elif origin_file.upper().endswith('PDF'):
            convert_files = []
            for idx, page in enumerate(convert_from_path(origin_file)):
                convert_tmp_file = '{}/{}.{}.tmp.jpg'.format(convert_dir, fname, str(idx))
                convert_file = '{}/{}.{}.jpg'.format(convert_dir, fname, str(idx))
                print('pdf > jpg - {} > {}'.format(origin_file, convert_tmp_file))
                page.save(convert_tmp_file, 'JPEG')
                resize_image(convert_tmp_file, convert_file)
                convert_files.append(convert_file)
            os.remove(origin_file)
            row['convert_files'] = convert_files
            files.append(row)
        else:
            print('unsupported file: {}'.format(origin_file))
            pass

    return files


def resize_image(org_image, resized_image):
    '''이미지 가로 * 세로 픽셀 사이즈가 25M을 넘으면 사이즈를 줄인다.'''
    img = Image.open(org_image)
    width, height = img.size
    if width * height < 25000000:
        print('copy file: {} > {}'.format(org_image, resized_image))
        os.replace(org_image, resized_image)
    else:
        m = math.ceil(((width * height) / 25000000) * 100) / 100
        new_width = int(width / m)
        new_height = int(height / m)
        print('resize image: {} x {} > {} x {}'.format(width, height, new_width, new_height))
        new_img = img.resize((new_width, new_height), Image.ANTIALIAS)
        print('copy file: {} > {}'.format(org_image, resized_image))
        new_img.save(resized_image)


def upload_file_to_storage(storage_client: storage.Client, bucket: str, key: str,
                           origin_file: str) -> None:
    '''로컬 파일을 gcs로 업로드'''
    bucket_ref = storage_client.bucket(bucket)
    blob = bucket_ref.blob(key)
    blob.upload_from_filename(origin_file)


def download_file_from_s3(s3_client: boto3.client, s3_bucket: str, key: str,
                          origin_file: str) -> None:
    '''s3에서 파일을 다운로드'''
    s3_client.download_file(s3_bucket, key, origin_file)


def update(target: Namespace, action_project: str) -> None:
    '''결과 데이터를 업로드'''
    base_date = datetime.strptime(_execution_date(), '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')
    project = getattr(target.project, _env())
    table_id = '{}.{}.{}'.format(project, target.dataset, target.name)
    params = {'reference_date': base_date, 'project': project, 'table_id': table_id}
    script = _get_script('sql/update.sql', params)
    scopes = [
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials, _ = google.auth.default(quota_project_id=action_project, scopes=scopes)
    client = bigquery.Client(credentials=credentials, project=action_project)
    query_job = client.query(script)
    query_job.result()


def run() -> None:
    '''이미지의 기본 실행 펑션'''
    with open('config.json', 'r') as f:
        config = Namespace(**json.loads(f.read()))
        action_project = getattr(config.action.project, _env())
        bucket = getattr(config.bucket, _env())
        checkpoint = config.checkpoint
        target = config.target

    with open(_gowid_db_slave(), 'r') as f:
        gowid_db_slave = Namespace(**json.loads(f.read()))

    with open(_s3_stockholder(), 'r') as f:
        s3_stockholder = Namespace(**json.loads(f.read()))

    stockholderfile_rows = get_stockholderfile_rows(gowid_db_slave)
    stockholderfile_rows = copy_and_convert_files(stockholderfile_rows, bucket, s3_stockholder,
                                                  action_project)
    ocr_data = get_ocr_data(stockholderfile_rows, action_project)

    upload_ocr_data_to_checkpoint_table(ocr_data, checkpoint, action_project)
    update(target, action_project)


def _env() -> str:
    return os.environ['env']


def _execution_date() -> str:
    return os.environ['execution_date']


def _gowid_db_slave() -> str:
    return os.environ['gowid_db_slave']


def _s3_stockholder() -> str:
    return os.environ['s3_stockholder']


def _get_script(template_path, params=None):
    with open(template_path, "r") as f:
        template = Template(f.read())
        script = template.render(params)
        print("스크립트:\n{}".format(script))
        return script


if __name__ == '__main__':
    run()
