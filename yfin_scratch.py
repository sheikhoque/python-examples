import http.client

conn = http.client.HTTPSConnection("apidojo-yahoo-finance-v1.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "65a98d3319mshebd452ff76db386p12fadfjsn41b8dc354c61",
    'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }

conn.request("GET", "/market/get-watchlist-detail?userId=mamunbuet@yahoo.com&pfId=default_watchlist", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))