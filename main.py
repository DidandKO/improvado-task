import csv
import json
import xml.etree.ElementTree as ET
import os
from configuration import SOURCE_ROOT, RESULT_FOLDER


def get_info_csv(current_file):
    with open(current_file, newline='') as csv_file:
        csv_file_reader = csv.DictReader(csv_file)
        for row in csv_file_reader:
            yield row


def get_info_json(current_file):
    with open(current_file) as json_file:
        json_file_reader = json.load(json_file)
        for row in json_file_reader['fields']:
            yield row


def get_info_xml(current_file):
    with open(current_file):
        xml_file_reader = ET.parse(current_file)
        root = xml_file_reader.getroot()
        xml_result = {}
        for tag in root.findall('objects/object'):
            header = tag.attrib['name']
            for value in tag.findall('value'):
                xml_result[header] = value.text
        return xml_result


def create_tsv(table, fieldnames, file_name):
    with open(os.path.join(RESULT_FOLDER, file_name), 'w', newline='') as tsv_file:
        dialect = csv.Dialect
        dialect.delimiter = '\t'
        dialect.other_param = 'val'
        dialect.quoting = csv.QUOTE_NONE
        dialect.lineterminator = '\n'
        writer = csv.DictWriter(tsv_file, fieldnames=fieldnames, dialect=dialect)
        writer.writeheader()
        for row in table:
            writer.writerow(row)


'''
    Два следующих метода необходимы для выделения общих колонок среди всех данных файлов
    и для удаления лишней информации.
'''


def find_common_keys(rows):
    keys = None
    for i in range(1, len(rows)):
        keys = rows[-1 + i].keys() & rows[i].keys()
    return list(keys)


def clear_big_rows(rows, heads):
    for row in rows:
        for cell in row.copy():
            if cell not in heads:
                row.pop(cell)
    return rows


def advanced_operation(table):
    new_table = []
    dict_to_sum = {}
    for row in table:
        firstD = row['D1']
        secondD = row['D2']
        thirdD = row['D3']
        t_key = f'{firstD}{secondD}{thirdD}'
        values_to_sum = [row['M1'], row['M2'], row['M3']]
        if t_key not in dict_to_sum:
            dict_to_sum[t_key] = []
            for value in values_to_sum:
                dict_to_sum[t_key].append(int(value))
        else:
            for i in range(len(dict_to_sum[t_key])):
                dict_to_sum[t_key][i] += int(values_to_sum[i])
    for key in dict_to_sum: # {abc: [1, 2, 3]}
        new_row = {}
        for i, d in enumerate(key):
            new_row[f'D{i+1}'] = d
        for i, m in enumerate(dict_to_sum[key]):
            new_row[f'MS{i+1}'] = m
        new_table.append(new_row)
    new_table = sorted(new_table, key=lambda x: (x['D1'], x['D2']))
    return new_table


if __name__ == '__main__':
    table_to_tsv = []
    file_list = os.listdir(SOURCE_ROOT)
    for file in file_list:
        file = os.path.join(SOURCE_ROOT, file)
        if file.endswith(".csv"):
            for row in get_info_csv(file):
                s_row = dict(sorted(row.items()))
                table_to_tsv.append(s_row)
        elif file.endswith(".json"):
            for row in get_info_json(file):
                s_row = dict(sorted(row.items()))
                table_to_tsv.append(s_row)
        elif file.endswith(".xml"):
            row = get_info_xml(file)
            s_row = dict(sorted(row.items()))
            table_to_tsv.append(s_row)
        else:
            print('Неизвестный формат файла')
    headers = sorted(find_common_keys(table_to_tsv))
    clear_big_rows(table_to_tsv, headers)
    table_to_tsv = sorted(table_to_tsv, key=lambda x: x['D1'])
    create_tsv(table_to_tsv, headers, 'basic_result.tsv')
    table_to_tsv = advanced_operation(table_to_tsv)
    advanced_headers = ['D1', 'D2', 'D3', 'MS1', 'MS2', 'MS3']
    create_tsv(table_to_tsv, advanced_headers, 'advanced_result.tsv')
