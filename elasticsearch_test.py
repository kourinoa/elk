from elasticsearch import Elasticsearch
import elasticsearch.helpers
import json


def car_data(es: Elasticsearch):
    es.indices.create(index="car", ignore=400)
    with open("./usedcar4.json", "r", encoding="utf-8") as file:
        count = 0
        data_list = []
        for line in file.readlines():
            # print(line)
            data = json.loads(line)
            data_list.append(data)
            count += 1
            if count % 500 == 0:
                actions = [{'_op_type': 'index', '_index': "car", '_type': "doc", '_source': d} for d in data_list]
                result = elasticsearch.helpers.bulk(es, actions)
                # result = es.index(index="car", body=data_list)
                data_list = []
                print(result)


def main():
    es = Elasticsearch(hosts=["localhost:9200"])
    car_data(es)
    # es.indices.create(index="car", ignore=400)  # 建立index

    # with open("/Users/Mac/elk/elk_start/3nd_byboard.csv", "r", encoding="utf-8") as inputCSV:
    #     header = inputCSV.readline()
    #     print(header)
    #     has_content = True
    #     count = 0
    #     while True:
    #         tmp = inputCSV.readline()
    #         if tmp is None:
    #             break
    #         line = tmp.split(",")
    #         try:
    #             for i in range(int(int(line[2]) / 10)):
    #                 result = es.index(index="car", body={"brand": line[0], "word": line[1]})
    #         except ValueError as interr:
    #             print(interr)
    #             print(line)
    #         count += 1
    #         if count % 1000 == 0:
    #             print(count, result)


if __name__ == '__main__':
    main()
