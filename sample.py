# -*- coding: utf-8 -*-
import argparse
import logging
import json
import apache_beam as beam
import os
from datetime import datetime
from pytz import timezone
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from google.cloud import storage
from apache_beam.coders.coders import Coder

# class DataIngestion:
#     def __init__(self, schema_path):
#         with open(schema_path) as f:
#             data = f.read()
#             self.schema_str = '{"fields" : ' + data + '}'
#
#     def read_json(self, data_path):
#         with open(data_path,'r') as f:
#             contents = json.load(f)
#         return contents


class DataIngestion:
    def __init__(self):
        dir_path = os.path.abspath(os.path.dirname(__file__))
        self.schema_str = ""
        # schema_file = os.path.join(dir_path, "schema", "gcp-webinar-schema.json")
        # os_path = os.path.abspath(os.path.dirname(__file__))
        #
        # with open(schema_file) \
        #         as f:
        #     data = f.read()
        #     self.schema_str = '{"fields": ' + data + '}'

        client = storage.Client()
        # get bucket with name
        bucket = client.get_bucket('gsn-dataflow-storage')

        # get bucket data as blob
        blob = bucket.get_blob('gcp-webinar-schema.json')

        # convert to string
        json_data = blob.download_as_string()
        json_data = json_data.decode('utf-8')
        print(json_data)
        print(type(json_data))
        self.schema_str = '{"fields": ' + json_data + '}'



class CustomCoder(Coder):
    """A custom coder used for reading and writing strings as UTF-8."""

    def encode(self, value):
        return value.encode("cp-949", "replace")

    def decode(self, value):
        return value.decode("cp-949", "ignore")

    def is_deterministic(self):
        return True


class BreakList(beam.DoFn):
    def __init__(self, delimiter=','):
        self.result = {}
        self.window = beam.window.GlobalWindow()

    def process(self, element):

        pos_code = element.find(',')

        pos_code_2 = element.find(":")

        if pos_code == -1 or pos_code_2 == -1:
            return
        if element[pos_code_2 + 1] == '"' and element[pos_code - 1] == '"':
            self.result[element[7:pos_code_2 - 1]] = element[pos_code_2 + 2:pos_code - 1]  # 숫자
        else:
            self.result[element[7:pos_code_2 - 1]] = element[pos_code_2 + 1:pos_code]  # 문자

        yield beam.utils.windowed_value.WindowedValue(
            value=self.result,
            # value = self.pre_processing,
            timestamp=0,
            windows=[self.window],
        )


def preprocessing(element):
    list = element.split(",")
    year = list[1][:4]
    year2 = list[2][:4]

    if (year=='2020' and year2=='2020') :
        return element


class makeBqTable(beam.DoFn) :
    def __init__(self, default="현재 설정값 없음"):
        self.default =default

    def process(self, fields):
        fields = fields.split(",")
        if fields[0]=='1':
            fields[0]='A'
        elif fields[0]=='2':
            fields[0]='B'
        elif fields[0]=='3':
            fields[0]='C'
        elif fields[0]=='4':
            fields[0]='D'
        elif fields[0]=='5':
            fields[0]='D'
        else :
            fields[0]='E'

        tablerow = {
                "VendorID" : fields[0],
                "tpep_pickup_datetime" : fields[1],
                "tpep_dropoff_datetime" : fields[2],
                "passenger_count": fields[3],
                "trip_distance": fields[4],
                "RatecodeID": fields[5],
                "store_and_fwd_flag": fields[6],
                "PULocationID": fields[7],
                "DOLocationID": fields[8],
                "payment_type": fields[9],
                "fare_amount": fields[10],
                "extra": fields[11],
                "mta_tax": fields[12],
                "tip_amount": fields[13],
                "tolls_amount": fields[14],
                "improvement_surcharge": fields[15],
                "total_amount" : fields[16],
                "congestion_surcharge" : fields[17],
        }
        yield tablerow

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser(description="Run Pipeline on the Cloud")

    parser.add_argument("--input",
                        dest="input",
                        help="Input file to read. This can be a local file or"
                             "a file in a Google Storage Bucket.",
                        # default="C:/Users/DW-PC/Desktop/IT기획 업무/02. 운영실적/preprocessing_data/desk.json")
                        )
    parser.add_argument("--output",
                        dest="output",
                        help="Output BQ table to write results to.",
                        default='tlc_taxi_trip_data.TEST_DB')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_args.extend([
        "--project=gsneotek-webinar",
        "--region=asia-northeast3",
        "--worker_region=asia-northeast3",
        "--machine_type=n1-standard-4",
        "--num_workers=1",
        "--max_num_workers=1",
        "--worker_disk_type=compute.googleapis.com/projects//zones//diskTypes/pd-ssd",
        #"--runner=DirectRunner",
        "--runner=DataflowRunner",
        "--staging_location=gs://nyc-tlc-yellow-data/dataflow-temp",
        "--temp_location=gs://nyc-tlc-yellow-data/dataflow-temp",
        # "--no_use_public_ips",
        "--save_main_session",
        "--autoscalingAlgorithm=THROUGHPUT_BASED",
        # "--subnetwork=https://www.googleapis.com/compute/v1/projects/dongwon-poc/regions/asia-northeast3/subnetworks/private-dongwon-subnet"
    ])


    filename = "gs://{}/*".format("nyc-tlc-yellow-data")
    data_ingestion = DataIngestion()
    schema = parse_table_schema_from_json(data_ingestion.schema_str)
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    list_do = (p
               | 'Read from a File' >> beam.io.ReadFromText(filename, skip_header_lines=1)

               | "Cleansing Data" >> beam.Filter(preprocessing)
               | "Make Table" >> beam.ParDo(makeBqTable(','))
               #| "Print" >> beam.Map(print)
               )
    # 리스트 내부의 데이터 하나씩 넘기기 위해 element 단위로 데이터 받기

    list_do | "Write" >> beam.io.Write(
        beam.io.BigQuerySink(
            # The table name is a required argument for the BigQuery sink.
            # In this case we use the value passed in from the command line.
            known_args.output,
            # Here we use the simplest way of defining a schema:
            # fieldName:fieldType
            schema=schema,
            # Creates the table in BigQuery if it does not yet exist.
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            # Deletes all data in the BigQuery table before writing.
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()