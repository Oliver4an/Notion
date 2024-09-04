from flask import Flask, jsonify, request 
import time ,requests ,json
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 500)
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

#NOTION_API_TOKEN
NOTION_API_TOKEN= 'secret_sS3wjhgH0z8yPr2DQODYaHmbobxxNOoQR7lT6W26nHo'

# 数据库 ID
DATABASE_ID = '008bfbbc8dd84fdb9180b63cea4d49fe'

# JUNK
JUNK_DB_ID = '0331a6763be3453695309493f8c4e36c'

junk_can = []

# 请求头
headers = {
    'Authorization': f'Bearer {NOTION_API_TOKEN}',
    'Content-Type': 'application/json',
    'Notion-Version': '2022-06-28'  # 确保使用最新的版本
}

#[read,write,sys]

def p_log(msg,type):
    tp = ''
    file_path = "./log.txt"

    if(type[0] == 1):
        tp = 'read'
    elif(type[1] == 1):
        tp = 'write'
    else:
        tp = 'system'

    output = "[%s] [%s] --> %s \n" % (str(datetime.now()) , tp ,msg )

    try:
        with open(file_path, 'a') as file:
            file.write(output + '\n')  # Append data followed by a newline
        # print(f"Data successfully appended to {file_path}")
    except IOError as e:
        print(f"An error occurred while writing to the file: {e}")

    print(output)

def db_req(DATABASE_ID,fname):
    type = [1,0,0]
    # API endpoint
    url = f'https://api.notion.com/v1/databases/{DATABASE_ID}/query'

    # 发起请求
    response = requests.post(url, headers=headers, data=json.dumps({}))

    # 检查响应状态
    if response.status_code == 200:
        data = response.json()
        # 打印返回的 JSON 数据
        # json_str = json.dumps(data, indent=4)
        # 將字符串寫入到 TXT 文件
        with open('./%s.json' % fname, 'w') as file:
            json.dump(data, file, indent=4)
        p_log("%s.JSON has been written into -> %s.json" % (fname, fname),type)
        # print("%s.JSON has been written into -> %s.json" % (fname, fname))

        # df = pd.json_normalize(data)
        # 顯示 DataFrame
        # print(df[df.columns])
    elif response.status_code == 400:
        p_log(f'Error 400: Bad Request. Here is the response content: \n %s ' % response.json(),type)
        # print(f'Error 400: Bad Request. Here is the response content:')
        # print(response.json())  # 打印详细的错误信息
    else:
        p_log(f'Error: {response.status_code} \n %s ' % response.text,type)
        # print(f'Error: {response.status_code}')
        # print(response.text)  # 打印响应的原始内容

# 定義更新資料的函數
def update_notion_page(page_id, property_name, new_value):
    type = [0,1,0]   

    url = f"https://api.notion.com/v1/pages/{page_id}"
    
    # 構建更新請求的 payload
    payload = {
        "properties": {
            property_name: {
                "rich_text": [
                    {
                        "text": {
                            "content": new_value
                        }
                    }
                ]
            }
        }
    }
    
    # 發送 PUT 請求以更新頁面
    response = requests.patch(url, headers=headers, data=json.dumps(payload))
    
    # 檢查請求是否成功
    if response.status_code == 200:
        p_log("Page updated successfully!",type)
        # print("Page updated successfully!")
    else:
        p_log(f"Failed to update page. Status code: {response.status_code} \n {response.text}",type)
        # print(f"Failed to update page. Status code: {response.status_code}")
        # print(response.text)


# Custom Function
def datalize():
    type = [0,0,1]

    try:
    # 讀取 JSON 文件並存入變數
        with open('./KDB.json', 'r') as file:
            data = json.load(file)

        # 顯示變數內容
        # print("讀取的數據:", data)
        
        data = data['results']
        # print(data)

        junk_can =[]
        # 創建 DataFrame
        i = 0
        for item in data:
            id = data[i]["id"]
            title = item["properties"]["Title"]["title"][0]["plain_text"]
            label = item["properties"]["Label"]["multi_select"][0]["name"] if item["properties"]["Label"]["multi_select"] else None
                
            viewed = item["properties"]["Viewed"]["rich_text"][0]["plain_text"] if item["properties"]["Viewed"]["rich_text"] else "0"
            clicked = item["properties"]["Clicked"]["formula"]["number"]
            is_update = True if int(clicked) != 0 else False
            junk_can.append({"id":id,"Title": title,"Label":label,"Viewed":viewed,"Clicked": clicked , "is_update":is_update})
            i+=1

        df = pd.DataFrame(junk_can)
        df =  df.sort_index(ascending=False).reset_index(drop=True)

        p_log("\n %s" %df,type)
        # print(df)
        return df

    except FileNotFoundError:
        p_log("錯誤: 文件不存在",type)
        # print("錯誤: 文件不存在")
    except json.JSONDecodeError:
        p_log("錯誤: 無法解析 JSON",type)
        # print("錯誤: 無法解析 JSON")
    except Exception as e:
        p_log(f"發生錯誤: {e}")
        # print(f"發生錯誤: {e}")


def junk_collection() -> bool:
    type = [0,0,1]

    try:
    # 讀取 JSON 文件並存入變數
        with open('./JDB.json', 'r') as file:
            data = json.load(file)

        # 顯示變數內容
        # print("讀取的數據:", data)
        
        data = data['results']
        # print(data)
        
        # 創建 DataFrame
        i = 0
        for item in data:
            id = data[i]["id"]
            junk_can.append(id)
            i+=1
   
        # print(junk_can[0]['data'])
        
    
    except FileNotFoundError:
        p_log("錯誤: 文件不存在",type)
        # print("錯誤: 文件不存在")
    except json.JSONDecodeError:
        p_log("錯誤: 無法解析 JSON",type)
        # print("錯誤: 無法解析 JSON")
    except Exception as e:
        p_log(f"發生錯誤: {e}")
        # print(f"發生錯誤: {e}")

def delete_page(junk_can) -> bool:
    type = [0,1,0]
    for id  in junk_can:
        url = f"https://api.notion.com/v1/pages/{id}"
    
        # 構建更新請求的 payload
        payload = {
            "archived":True
        }
        
        # 發送 PUT 請求以更新頁面
        response = requests.patch(url, headers=headers, data=json.dumps(payload))
        
        # 檢查請求是否成功
        if response.status_code == 200:
            p_log("Junk page %s deleted successfully!" % id,type)
            time.sleep(5)
            # print("Junk page %s deleted successfully!" % id,type)
        else:
            p_log(f"{id} Failed to deleted Junk page. Status code:  {response.status_code} \n {response.text}",type)
            # print(f"%s Failed to deleted Junk page. Status code:  {response.status_code}" % id)
            # print(response.text)

        

def update(df):
    type = [0,0,1]
    # `page_id` 是要更新的頁面 ID，`property_name` 是欄位名稱，`new_value` 是新的值
    for idx in range(len(df)):
        if(df.iloc[idx]['is_update']):    
            page_id = df.iloc[idx]['id']
            property_name = 'Viewed'
            new_value = str(int(df.iloc[idx]['Viewed'])+int(df.iloc[idx]['Clicked']))
            # print(page_id,property_name,new_value)
            p_log(f"{page_id},{property_name},{new_value}",type)

            # 獲取並打印所有頁面的 ID
            update_notion_page(page_id, property_name, new_value)


def main():
    # t = 0
    # print("Time Quantum: %d\n" % t)
    db_req(JUNK_DB_ID,"JDB")
    junk_collection()
    p_log("jc: %s" % junk_can,type)
    # print("jc: %s" % junk_can)
    if(len(junk_can)) > 0:
        t = 10
        db_req(JUNK_DB_ID,"JDB")
        db_req(DATABASE_ID,"KDB")
        update(datalize())
        delete_page(junk_can)
    # else:
        # t = 0
        # time.sleep(t)   
        
    junk_can.clear()
            


        

def scheduled_task():
    print("Scheduled task running...")
    main()

scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_task, trigger="interval", seconds=45)
scheduler.start()

@app.route('/')
def home():
    return "Flask is running with a scheduled task!"




if __name__ == '__main__':
    try:
        app.run(debug=True)
    except (KeyboardInterrupt, SystemExit):
        # Shut down the scheduler gracefully when exiting the app
        scheduler.shutdown()