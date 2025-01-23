FROM python:3.12.2

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --upgrade psycopg2-binary

COPY . .

CMD ["python", "bravo.py"]
