from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import tarfile
import csv
from io import TextIOWrapper, BytesIO
import os

app = FastAPI()

# CORS 設定
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 固定讀取位於 app.py 目錄內的 member.tar.gz 檔案
@app.get("/read_member_tar_gz/")
async def read_member_tar_gz():
    file_path = os.path.join(os.path.dirname(__file__), 'member.tar.gz')
    
    if os.path.exists(file_path):
        try:
            with tarfile.open(file_path, mode="r:gz") as tar:
                member = {}
                for tarinfo in tar:
                    if tarinfo.isfile() and tarinfo.name.endswith(".csv"):
                        csv_file = tar.extractfile(tarinfo)
                        csv_reader = csv.DictReader(TextIOWrapper(csv_file, encoding='utf-8'))
                        for row in csv_reader:
                            name = row['name']
                            priority = row['priority']
                            member[name] = priority
                return {"status": "success", "member": member}
        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}


# 新增讀取 member.csv 的 API
@app.get("/read_member_csv/")
async def read_member_csv():
    file_path = os.path.join(os.path.dirname(__file__), 'member.csv')
    
    if os.path.exists(file_path):
        try:
            with open(file_path, mode='r', encoding='utf-8') as csvfile:
                csv_reader = csv.DictReader(csvfile)
                member = {}
                for row in csv_reader:
                    name = row['name']
                    priority = row['priority']
                    member[name] = priority
                return {"status": "success", "member": member}
        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": f"{file_path} not found."}


def process_large_csv(file_obj, chunk_size=1024 * 1024):
    member = {}
    # 用csv.reader按行讀取文件，分塊處理
    reader = csv.DictReader(file_obj)
    chunk = []
    for row in reader:
        chunk.append(row)
        if len(chunk) >= chunk_size:
            # 處理當前塊
            for data in chunk:
                name = data['name']
                priority = data['priority']
                member[name] = priority
            chunk = []
    # 處理最後未滿chunk_size的塊
    if chunk:
        for data in chunk:
            name = data['name']
            priority = data['priority']
            member[name] = priority
    return member

@app.post("/chunk_upload_gzip/")
def upload_gzip(file: UploadFile = File(...)):
    try:
        if file.filename.endswith(".tar.gz"):
            with tarfile.open(fileobj=BytesIO(file.file.read()), mode="r:gz") as tar:
                for tarinfo in tar:
                    if tarinfo.isfile() and tarinfo.name.endswith(".csv"):
                        csv_file = tar.extractfile(tarinfo)
                        return {"status": "success", "member": process_large_csv(TextIOWrapper(csv_file, encoding='utf-8'))}
        elif file.filename.endswith(".gz"):
            with gzip.GzipFile(fileobj=BytesIO(file.file.read())) as gz:
                return {"status": "success", "member": process_large_csv(TextIOWrapper(gz, encoding='utf-8'))}
        else:
            return {"status": "failed", "message": "Please upload a valid .gz or .tar.gz file."}
    except Exception as e:
        return {"status": "failed", "message": str(e)}


@app.post("/stream_upload_gzip/")
def upload_gzip(file: UploadFile = File(...)):
    if file.filename.endswith(".gz"):
        try:
            file_bytes = file.file.read()  # 同步讀取文件
            if file.filename.endswith(".tar.gz"):
                with tarfile.open(fileobj=BytesIO(file_bytes), mode="r:gz") as tar:
                    member = {}
                    for tarinfo in tar:
                        if tarinfo.isfile() and tarinfo.name.endswith(".csv"):
                            csv_file = tar.extractfile(tarinfo)
                            # 使用流式處理，逐行讀取 CSV 文件
                            for row in csv.DictReader(TextIOWrapper(csv_file, encoding='utf-8')):
                                member[row['name']] = row['priority']
                    return {"status": "success", "member": member}
            else:
                with gzip.GzipFile(fileobj=BytesIO(file_bytes)) as gz:
                    member = {}
                    # 使用流式處理逐行讀取
                    for row in csv.DictReader(TextIOWrapper(gz, encoding='utf-8')):
                        member[row['name']] = row['priority']
                    return {"status": "success", "member": member}
        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": "Please upload a valid .gz or .tar.gz file."}


# 上傳 gz 文件的 API (保持原有)
@app.post("/upload_gzip/")
async def upload_gzip(file: UploadFile = File(...)):
    if file.filename.endswith(".gz"):
        try:
            file_bytes = await file.read()
            if file.filename.endswith(".tar.gz"):
                with tarfile.open(fileobj=BytesIO(file_bytes), mode="r:gz") as tar:
                    member = {}
                    for tarinfo in tar:
                        if tarinfo.isfile() and tarinfo.name.endswith(".csv"):
                            csv_file = tar.extractfile(tarinfo)
                            csv_reader = csv.DictReader(TextIOWrapper(csv_file, encoding='utf-8'))
                            for row in csv_reader:
                                member[row['name']] = row['priority']
                    return {"status": "success", "member": member}
            else:
                with gzip.GzipFile(fileobj=BytesIO(file_bytes)) as gz:
                    member = {}
                    for row in csv.DictReader(TextIOWrapper(gz, encoding='utf-8')):
                        member[row['name']] = row['priority']
                    return {"status": "success", "member": member}
        except Exception as e:
            return {"status": "failed", "message": str(e)}
    else:
        return {"status": "failed", "message": "Please upload a valid .gz or .tar.gz file."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=5000, reload=True)