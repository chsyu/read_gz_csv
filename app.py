from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import tarfile
import csv
from io import TextIOWrapper
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