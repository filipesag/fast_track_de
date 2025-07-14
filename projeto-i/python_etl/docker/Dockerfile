FROM python:latest

WORKDIR /app

COPY  requirements.txt  ./

RUN pip install --no-cache-dir -r requirements.txt

COPY  python_etl/ ./

CMD ["python", "etl.py"]