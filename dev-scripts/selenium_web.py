from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.chrome.service import Service

"""Test selenium for tennis atprour website scraping --> not working because of captcha's"""
"""Connexion to the website"""

options = Options()
# options.add_argument("--headless")
options.add_argument(r"user-data-dir=C:\Users\kevec\AppData\Local\Google\Chrome\User Data\Default")

browser = webdriver.Chrome(r'C:\Users\chkev\chromedriver.exe')

browser.get('https://www.atptour.com/en/rankings/singles')
# browser.get('http://www.google.com/')


"""Create csv file"""

tennis_csv= open('Tennis_player_details_scrape.csv', 'w')

tennis_csv.write('Player' + ',' + 'Career_length'+ ',' +  'Aces' + ',' + 'Double_faults' + ',' + 'First_serve' + ',' + '1st_Serve_Points_Won'
                             + ',' + '2nd_Serve_Points_Won' + ',' + 'Break_Points_Faced' + ',' + 'Break_Points_Saved' + ',' + 'Service_Games_Played' + ',' +
                             'Service_Games_Won' + ',' + 'Total_Service_Points_Won' + ',' + '\n')


"""Writing first 20 players of the single ranking"""

for i in range(1, 5):
    #Get link to each player and write name of the player in csv file
    player = WebDriverWait(browser, 20).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="rankingDetailAjaxContainer"]/table/tbody/tr['+str(i)+']/td[4]/span/a')))
    print(player)
    tennis_csv.write(player.text + ',')

    #click on each generated link to go to player's bio, get "turnet pro" info and write it to the file
    WebDriverWait(browser, 100).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="rankingDetailAjaxContainer"]/table/tbody/tr['+str(i)+']/td[4]/span/a'))).click()
    turned_pro = WebDriverWait(browser, 100).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="playerProfileHero"]/div[2]/div[2]/div/table/tbody/tr[1]/td[2]/div/div[1]'))).text
    print(turned_pro)
    career_length = 2019 - int(turned_pro)
    tennis_csv.write(str(career_length) + ',')

    WebDriverWait(browser, 100).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="profileTabs"]/ul/li[6]/a'))).click()
    WebDriverWait(browser, 100).until(expected_conditions.visibility_of_element_located((By.CSS_SELECTOR, 'div[id="playerMatchFactsContainer"]')))


"""Testing on ultimate tennis website"""

options = Options()
options.add_argument("--headless")
#options.add_argument(r"user-data-dir=C:\Users\kevec\AppData\Local\Google\Chrome\User Data\Default")

browser = webdriver.Chrome(r'C:\Users\chkev\chromedriver.exe', options=options)

browser.get('https://www.ultimatetennisstatistics.com/rankingsTable')


#for i in range(1, 5):
#Get link to each player and write name of the player in csv file
player = WebDriverWait(browser, 20).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="rankingsTable"]/tbody/tr[1]/td[4]/a')))
print(player)
tennis_csv.write(player.text + ',')

# click on each generated link to go to player's bio, get "turnet pro" info and write it to the file
WebDriverWait(browser, 100).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="rankingsTable"]/tbody/tr[1]/td[4]/a'))).click()
turned_pro = WebDriverWait(browser, 100).until(expected_conditions.visibility_of_element_located((By.XPATH, '//*[@id="playerProfileHero"]/div[2]/div[2]/div/table/tbody/tr[1]/td[2]/div/div[1]'))).text
print(turned_pro)
career_length = 2019 - int(turned_pro)
tennis_csv.write(str(career_length) + ',')

"""Other method to click (close to JS)"""


# element = browser.find_elements_by_tag_name("a")
# print(type(element))
# for i in element:
#     if i.text == "Novak Djokovic":
#         query = i
#         print(i)
#         print(i.text)

empty_list = []
prize_money = []
for i in range(1, 3):
    player = browser.find_elements_by_xpath(f'//*[@id="rankingsTable"]/tbody/tr[{i}]/td[4]/a')
    print(type(player))
    print(type(player))
    for i in player:
        print(i.text)
        empty_list.append(i.text)
        i.click()
        money = browser.find_elements_by_xpath('//*[@id="profile"]/div/div[1]/table/tbody/tr[14]/td')
        prize_money.append(money)
        browser.back()

print(empty_list)
print(prize_money)

    # for top in djoko:
    #     click_on_it = top
    #     print(top)
    #     print(top.text)

# player = browser.find_elements_by_xpath('//*[@id="profile"]/div/div[1]/table/tbody/tr[4]/td')
# print(type(players))
# for i in players:
#     print(i.text)
