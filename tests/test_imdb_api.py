import requests

url = "https://imdb8.p.rapidapi.com/title/find"

querystring = {"q":"fight club"}

headers = {
    'x-rapidapi-key': "c9a486b794msh0ed978fc19efe93p11f9fdjsn34a363c2926e",
    'x-rapidapi-host': "imdb8.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)
print(type(response))
response_txt = response.json()
print(type(response_txt))
print(response_txt.get("results"))
for k,v in response_txt.items():
    print(k)
    print(v)



import requests

url = "https://tennis-data1.p.rapidapi.com/tennis/tournaments"

querystring = {"minPrize":"1000000","year":"2020","page":"1","type":"ATP"}

headers = {
    'x-rapidapi-key': "c9a486b794msh0ed978fc19efe93p11f9fdjsn34a363c2926e",
    'x-rapidapi-host': "tennis-data1.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)