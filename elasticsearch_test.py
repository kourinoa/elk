from elasticsearch import Elasticsearch
import elasticsearch.helpers
import json
import datetime
import requests

location_dict = {}


def create_idx(es: Elasticsearch):
    # 定義index 資料形態
    mappings = {
        "mappings": {
            "properties": {
                'id': {"type": "text"},
                'source': {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                'brand': {"type": "text",
                          "fields": {
                              "keyword": {"type": "keyword"}
                          }
                          },
                'type': {"type": "text",
                         "fields": {
                             "keyword": {"type": "keyword"}
                         }
                         },
                'link': {"type": "text"},
                'cc': {"type": "float"},
                'gas': {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                'sys': {"type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                        },
                'color': {"type": "text",
                          "fields": {
                              "keyword": {"type": "keyword"}
                          }
                          },
                'miles': {"type": "float"},
                'year': {"type": "date"},
                'price': {"type": "float"},
                'locate': {"type": "text"},
                "location": {"type": "geo_point"},
                'power': {"type": "float"},
                'l_chair': {"type": "float"},
                'seller': {"type": "text"},
                'back_screen': {"type": "float"},
                'air_con': {"type": "float"},
                'safe_bag': {"type": "float"},
                'gps': {"type": "float"},
                'ss': {"type": "float"}
            }
        }
    }
    # 建立index ，如果index已經存在提示 400 警告
    print(es.indices.create(index="car", body=mappings, ignore=400))
    print(es.indices.get(index="car"))


def car_data(es: Elasticsearch):
    batch_size = 500  # 一次寫入es的檔案數量
    # 讀取資料
    with open("./usedcar4.json", "r", encoding="utf-8") as file:
        count = 0
        data_list = []
        for line in file.readlines():
            # print(line)
            data = json.loads(line)
            data["id"] = str(data["id"])  # id 轉 str
            data["year"] = datetime.datetime.strptime(str(data["year"]).split(".")[0], "%Y")
            # 地點轉換經緯度
            ll = get_lat_lng(data["locate"])["results"][0]["geometry"]['location']
            # print(ll)
            # data["location"] = "{},{}".format(ll["lat"], ll["lng"])
            data["location"] = {'lat': ll["lat"], 'lon': ll["lng"]}
            # data["location"] = [ll["lng"], ll["lat"]]
            data_list.append(data)
            count += 1
            # 一次寫 500 筆
            if count % batch_size == 0:
                try:
                    actions = [{'_op_type': 'index', '_index': "car", '_source': d} for d in data_list]
                    result = elasticsearch.helpers.bulk(es, actions)
                    # result = es.index(index="car", body=data_list)
                    data_list = []
                    print(result)
                except Exception as err:
                    print("*" * 50)
                    print("err msg: ", err)
                    print("err data: ", data_list)
                    print("err result: ", result)
                finally:
                    data_list = []


def load_location():
    try:
        with open("./location.json", "r", encoding="utf-8") as file:
            global location_dict
            location_dict = json.loads(file.read())
    except FileNotFoundError as err:
        print(err)
        print("./location.json not found")


def save_location():
    with open("./location.json", "w+", encoding="utf-8") as file:
        file.write(json.dumps(location_dict))


def get_lat_lng(location: str) -> dict:
    # 高得地圖########################################
    # params = {'address': location, 'key': 'a9ef79fb50eda48bc2f01ffb83ff1264'}
    # query_url = "https://restapi.amap.com/v3/geocode/geo"
    ################################################
    # google map####################################
    global location_dict
    # 檢查location cache 是否存在
    if location_dict is None:
        load_location()
    # 從 cache 中取得地點
    return_location = location_dict.get(location, None)
    if return_location is not None:
        return return_location
    else:  # 如果cache中沒有地點，從google map api取得地點
        params = {'address': location, 'key': 'AIzaSyBtfqr_U68vrjF_YS-OieTtIslz6DfbFtk', 'language': 'zh-TW'}
        query_url = "https://maps.googleapis.com/maps/api/geocode/json"
        ################################################
        res = requests.get(url=query_url, params=params)
        if res.status_code == 200:
            # print("request : ", res.url)
            loc = res.json()
            location_dict[location] = loc
            save_location()
            return loc


def main():
    es = Elasticsearch(hosts=["kvsr.ddns.net:19200"])
    load_location()
    create_idx(es)
    car_data(es)
    # # print( get_lat_lng("臺中市")["results"][0]["geometry"]['location'] )
    save_location()

    # with open("./usedcar4.json", "r", encoding="utf-8") as file:
    #     for line in file.readlines():
    #         print(json.dumps(json.loads(line), indent=4, ensure_ascii=False))
    #         break
    # print(datetime.datetime.strptime("2011.0".split(".")[0], "%Y"))
    # print("2011.0".strip("."))
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
