import apache_beam as beam
import json

PROJECT = 'covid-1'
BUCKET = 'receiptsocrcgfbucket'


def run():
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=receiptocr',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/tmp/'.format(BUCKET),
        '--runner=DataflowRunner'
    ]

    p = beam.Pipeline(argv=argv)
    table_spec = 'covid-1:receipts.receipts'
    input = 'gs://{0}/ocr_results/*.json'.format(BUCKET)

    table_schema = {
        'fields': [
            {
                'name': 'shop', 'type': 'FLOAT', 'mode': 'NULLABLE'
            }, {
                'name': 'day', 'type': 'STRING', 'mode': 'NULLABLE'
            }, {
                'name': 'month', 'type': 'STRING', 'mode': 'NULLABLE'
            }, {
                'name': 'year', 'type': 'STRING', 'mode': 'NULLABLE'
            }, {
                'name': 'total', 'type': 'FLOAT', 'mode': 'NULLABLE'
            }
        ]
    }

    (p
     | 'getInputFromGCS' >> beam.io.ReadFromText(input)
     | 'toJson' >> beam.Map(lambda x: json.loads(x))
     | 'writeBQ' >> beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
     )

    p.run()


if __name__ == '__main__':
    run()
