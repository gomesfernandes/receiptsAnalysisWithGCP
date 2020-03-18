import json
import logging

import apache_beam as beam

PROJECT = 'covid-1'
BUCKET = 'receiptsocrcgfbucket'
TOPIC_NAME = 'new_receipt_in_json_format'


class LoggerFn(beam.DoFn):
    def process(self, element):
        logging.info("element looks like this:" + str(element))
        yield element


def run():
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=receiptocr-streaming',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/tmp/'.format(BUCKET),
        '--runner=DataflowRunner',
        '--streaming'
    ]

    table_spec = 'covid-1:receipts.receipts'
    input = 'gs://{0}/{1}'
    topic = 'projects/{}/topics/{}'

    table_schema = {
        'fields': [
            {
                'name': 'shop', 'type': 'STRING', 'mode': 'NULLABLE'
            }, {
                'name': 'date', 'type': 'DATE', 'mode': 'NULLABLE'
            }, {
                'name': 'total', 'type': 'FLOAT', 'mode': 'NULLABLE'
            }
        ]
    }

    with beam.Pipeline(argv=argv) as p:
        (p
         | 'readTopic' >> beam.io.ReadFromPubSub(topic=topic.format(PROJECT, TOPIC_NAME))
         | 'decodeFilename' >> beam.Map(lambda x: x.decode('utf-8'))
         | 'toFilename' >> beam.Map(lambda x: input.format(BUCKET, x))
         | 'getInputFromGCS' >> beam.io.ReadAllFromText()
         | 'toJson' >> beam.Map(lambda x: json.loads(x))
         | 'log' >> beam.ParDo(LoggerFn())
         | 'writeBQ' >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=table_schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
         )


if __name__ == '__main__':
    run()
