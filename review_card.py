#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 23:06:05 2024

@author: johnomole
"""
import logging
import os
import csv
import time
from datetime import datetime
from bs4 import BeautifulSoup
from requests import request

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
LOG = logging.getLogger("KFC Review card")
LOG.setLevel(os.environ.get("LOG_LEVEL", logging.DEBUG))


"""
The script crawl the trustpilot and download 200 reviews regarding kfc and save it in a csv file called `review_card.csv`
You can control the script to download all the reviews related to kfc
"""

class ReviewCard:
    def __init__(self):
        """

        Returns
        -------
        None.

        """
        self.base_url = 'https://www.trustpilot.com/review/www.kfc.com'

    def review_cralwer(self):
        """

        Returns
        -------
        data : TYPE
            DESCRIPTION.

        """
        page = 0
        data = []
        while True:
            try:
                time.sleep(2)
                if page == 10: # This is to control the download and just get 200 review since we have 20 per page.
                    break
                page+=1
                url = f'{self.base_url}?page={page}'
                res = request("GET", url=url,headers={'user-agent': 'my-app/0.0.1'})
                html = BeautifulSoup(res.text)
                review_card = html.findAll("div", {"class":"styles_cardWrapper__LcCPA styles_show__HUXRb styles_reviewCard__9HxJJ"})
                data.extend(self.data_extractor(review_card))
            except Exception as e:
                print(e)
                print(f"probably last page: {page}")
                break
        return data

    def data_extractor(self, review_card):
        """

        Parameters
        ----------
        review_card : TYPE
            DESCRIPTION.

        Returns
        -------
        result : TYPE
            DESCRIPTION.

        """
        result = []
        for element in review_card:
            data = {}
            # User ID
            user_id_element = element.find('a')
            data["uuid"] = user_id_element['href'].split('/')[-1] if user_id_element else None
            
            # Name
            name_element = element.find("span", {"class": "typography_heading-xxs__QKBS8 typography_appearance-default__AAY17"})
            data["name"] = name_element.getText() if name_element else None
            
            # Topic
            topic_element = element.find("h2", {"class": "typography_heading-s__f7029 typography_appearance-default__AAY17"})
            data["topic"] = topic_element.getText() if topic_element else None
            
            # Message
            message_element = element.find("p", {"class": "typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn"})
            data["message"] = message_element.getText() if message_element else None
            
            # Date of Experience
            date_of_experience_element = element.find("p", {"class": "typography_body-m__xgxZ_ typography_appearance-default__AAY17"})
            data["date_of_experience"] = date_of_experience_element.getText().split(':')[-1].strip() if date_of_experience_element else None
            
            # Date of Review
            date_of_review_element = element.find("div", {"class": "typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_datesWrapper__RCEKH"})
            data["date_of_review"] = date_of_review_element.getText() if date_of_review_element else None
            
            # Star
            data["star"] = self.extra_star(element)
            result.append(data)
        return result

    def extra_star(self, element):
        """

        Parameters
        ----------
        element : TYPE
            DESCRIPTION.

        Returns
        -------
        alt_text : TYPE
            DESCRIPTION.

        """
        for img_tag in element.find_all('img'):
            alt_text = img_tag.get('alt')
            if alt_text:
                return alt_text

    def data_export(self, data):
        """

        Parameters
        ----------
        data : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        csv_file_path = 'review_card.csv'
        with open(csv_file_path, 'w', newline='') as file:
            csv_writer = csv.DictWriter(file, fieldnames = ['uuid', 'timestamp', 'message'])
            csv_writer.writeheader()
            for row in data:
                row = {'uuid': row['uuid'],
                       'timestamp':datetime.strptime(row['date_of_experience'],"%B %d, %Y"),
                       'message': row['message']}
                csv_writer.writerow(row)

if __name__ == "__main__":
    review_handler = ReviewCard()
    data = review_handler.data_export(review_handler.review_cralwer())