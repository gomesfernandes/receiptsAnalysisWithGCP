import io
import glob
import re
import json
from google.cloud import vision
from google.cloud import storage
from google.cloud import pubsub_v1

PROJECT_ID = 'covid-1'
TOPIC = 'new_receipt_in_json_format'
INPUT_BUCKET = 'receiptsocrcgfbucketimages'
OUTPUT_BUCKET = 'receiptsocrcgfbucket'


def fn_extract_json(event, context):
    """
    New receipt uploaded. Read the file, call vision API and save the json content to a new file.
    """
    filename = event['name']
    if not re.search(r'\.jpe?g', filename):
        print('File + ' + filename + ' is not an image')

    image_path = 'gs://{0}/{1}'.format(INPUT_BUCKET, filename)
    extracted_entities = detect_entities(image_path, True)
    if not extracted_entities:
        print('Nothing to extract from file ' + filename)

    client = storage.Client()
    bucket = client.get_bucket(OUTPUT_BUCKET)
    json_filename = re.sub(r'\.jpe?g', '.json', filename)
    blob = bucket.blob(re.sub(r'\.jpe?g', '.json', filename))
    blob.upload_from_string(json.dumps(extracted_entities))
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC)
    publisher.publish(topic_path, data=json_filename.encode('utf-8'))


def detect_entities(path, gcs_source=False):
    """Detects text in the file."""
    client = vision.ImageAnnotatorClient()

    if gcs_source:
        image = vision.types.Image(source=vision.types.ImageSource(image_uri=path))
        response = client.text_detection(image=image)
    else:
        with io.open(path, 'rb') as image_file:
            content = image_file.read()
        image = vision.types.Image(content=content)
        response = client.text_detection(image=image)

    data = response.full_text_annotation

    page = data.pages[0]
    lines = []
    for block in page.blocks:
        if vision.enums.Block.BlockType(block.block_type) != vision.enums.Block.BlockType.TEXT:
            continue
        for paragraph in block.paragraphs:
            line_content = ""
            line_start_vertices = paragraph.words[0].symbols[0].bounding_box.vertices[0]
            newline = False
            for word in paragraph.words:
                if newline:
                    lines.append(
                        (line_start_vertices.x, line_start_vertices.y,
                         line_end_vertices.x, line_end_vertices.y,
                         line_content)
                    )
                    line_content = ""
                    line_start_vertices = word.symbols[0].bounding_box.vertices[0]
                    newline = False
                w = ""
                for symbol in word.symbols:
                    w += symbol.text
                    if symbol.property and symbol.property.detected_break:
                        break_type = symbol.property.detected_break.type
                        breaks = vision.enums.TextAnnotation.DetectedBreak.BreakType
                        if (breaks(break_type) == breaks.EOL_SURE_SPACE or
                                breaks(break_type) == breaks.LINE_BREAK):
                            newline = True
                            line_end_vertices = symbol.bounding_box.vertices[1]
                        elif breaks(break_type) == breaks.SPACE:
                            w += ' '
                line_content += w
            if newline:
                lines.append(
                    (line_start_vertices.x, line_start_vertices.y,
                     line_end_vertices.x, line_end_vertices.y,
                     line_content)
                )

    if len(lines) == 0:
        return None

    fused_lines = []
    for line in lines:
        x1, y1, x2, y2 = line[0], line[1], line[2], line[3]
        if x2 - x1 < 0.0001:
            continue
        a = (y2 - y1) / (x2 - x1)
        b = y1 - (a * x1)
        line_info = {
            'a': a,
            'b': b,
            'content': line[4]
        }
        match = re.search(r'^\d+[\.,]\s{0,1}\d\d\s*€{0,1}$', line[4])
        was_fused = False
        if match:
            closest_line_index = fused_lines[len(fused_lines) - 1]
            closest_line_diff = 9999999
            for i in range(len(fused_lines)):
                y_line = fused_lines[i]['a'] * x1 + fused_lines[i]['b']
                actual_y = y1
                line_diff = abs(y_line - actual_y)
                if line_diff <= closest_line_diff:
                    closest_line_diff = line_diff
                    closest_line_index = i
            fused_lines[closest_line_index]['content'] += " "
            fused_lines[closest_line_index]['content'] += line[4]
            was_fused = True
        if not was_fused:
            fused_lines.append(line_info)

    if len(fused_lines) == 0:
        return None

    content = dict()
    content["shop"] = fused_lines[0]['content']

    for line in fused_lines:
        match = re.search(r'(\d{1,2})[/\-\.]([01]?\d)[/\-\.](\d{2,4})', line['content'])
        if match:
            day = match.group(1).rjust(2, '0')
            month = match.group(2).rjust(2, '0')
            year = match.group(3)
            if len(year) == 2:
                year = "20" + year
            content["date"] = year + "-" + month + "-" + day
            break

    for i, line in enumerate(fused_lines):
        if ('total' in line['content'].lower()) or ('tota7' in line['content'].lower()) or (
                'espèces' in line['content'].lower()):
            match = re.search(r'(\d+)[\.,]\s?(\d\d)\s*€?', line['content'])
            if match:
                content["total"] = float(match.group(1) + '.' + match.group(2))
                break

    return content


if __name__ == '__main__':
    files = glob.glob('personal_receipts/*.jpg')
    for f in files:
        print(detect_entities(f))