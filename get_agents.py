#! /usr/bin/env python3
from bs4 import BeautifulSoup
import requests


def get_agent_list():
    url = 'https://developers.whatismybrowser.com/useragents/explore/software_name/chrome/{}'
    
    agent_list = []
    for i in range(6):
        page = requests.get(url.format(i))
        soup = BeautifulSoup(page.text, 'html.parser')
        
        user_agents = soup.find_all('td', class_='useragent')
        
        for agent in user_agents:
            text = agent.text
            agent_list.append(text)
            
        print('Got a page of user-agents')

    return agent_list
    
