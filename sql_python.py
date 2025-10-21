#!/usr/bin/env python3
import requests
import urllib.parse

API_URL = "http://127.0.0.1:8081/query"  # server endpoint

def main():
    print("Enter query:")
    while True:
        query = input("> ").strip()
        if not query:
            print("Exit")
            break

        #encoded_query = urllib.parse.quote(query)
        url = f"{API_URL}?{query}"

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            print("Response:")
            print(response.text)
        except requests.RequestException as e:
            print("Error:", e)

if __name__ == "__main__":
    main()        