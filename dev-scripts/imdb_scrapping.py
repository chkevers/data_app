from imdb import IMDb
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
import tqdm

"""Test IMDB package --> good use case for full stack"""

movie = IMDb().get_movie('012346')
print(movie)

print(movie.items())

print(movie.keys())
print(movie.values())
print(movie.get("directors"))

shows = IMDb().get_popular100_tv()
for i in sorted(shows):
    print(i)
    print(i.getID())

test_show = IMDb().get_movie_external_reviews('0108778')
print(test_show)


"""Test selecnium for tennis atprour website scraping"""