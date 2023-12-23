from google.cloud.storage import Client, Bucket


if __name__ == '__main__':
    client = Client()
    print(list(client.list_buckets()))