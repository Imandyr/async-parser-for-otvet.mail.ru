import random
import time
import aiohttp
import asyncio
import aiofiles
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import numpy as np
import re
import json
from logger import logger

# removing error
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# cleaning input text from unnecessary characters
def cleaning_text(text):
    text = re.sub(r"\n", "", text)
    text = re.sub(r'"', r"'", text)
    text = re.sub(r'\r', r"", text)
    text = re.sub(r"\s+", " ", text)
    return text


# get html of target page
async def get_requests(url: str, headers=None, proxy=None):
    # try to connect
    try:
        # create session
        async with aiohttp.ClientSession() as session:
            # get html
            async with session.get(url, headers=headers, proxy=proxy) as response:
                # if connect
                if int(response.status) == 200 or int(response.status) == 404 or int(response.status) == 429:
                    # read html
                    response = await response.read()
                    # return html
                    return response
                # if page return error
                else:
                    # return error status
                    return str(response.status)

    # if connection failed
    except:
        # return blank value
        logger.info(f"connection error to {url}")
        return "None"


# get target data from html
async def get_data_from_page(html: str, question_number: int):
    # soup object
    soup = BeautifulSoup(html, "lxml")

    # parse all target objects

    # question
    #print(question_number)  # number of question
    question_category = soup.find("a", class_="black list__title list__title")  # category of question
    question_date = soup.find('time', itemprop="datePublished") # date of question publication
    question_head = soup.find('h1', class_="q--qtext") # header of question
    question_text = soup.find("div", class_="q--qcomment medium") # main text of question
    question_rating = soup.find("meta", itemprop="upvoteCount") # rating of question (in upvotes)
    question_author_name = soup.find("b", class_="fn nickname") # name of question author
    question_author_rating = soup.find("span", class_="author-rating") # rating of question author

    # answers
    answers_dates = soup.find_all('span', class_="published updated a--added") # dates of answers publication
    answers_texts = soup.find_all("div", class_="a--atext atext") # answers texts
    answers_ratings = soup.find_all("meta", itemprop="upvoteCount")[1:] # ratings of answers (in upvotes)
    answers_authors_names = soup.find_all("b", class_="fn nickname")[1:] # names of answers authors
    answers_authors_ratings = soup.find_all("span", class_="author-rating")[1:] # ratings of answers authors

    # processing (clean text of unnecessary chars and process)

    # question

    # question_category
    if question_category:
        question_category = cleaning_text(str(question_category.text))
    else:
        question_category = "<None>"

    # question_date
    if question_date:
        question_date = str(question_date)[16:41]
    else:
        question_date = "<None>"

    # question_head
    if question_head:
        question_head = cleaning_text(str(question_head.text))
    else:
        question_head = "<None>"

    # question_text
    if question_text:
        question_text = cleaning_text(str(question_text.text))
    else:
        question_text = "<None>"

    # question_rating
    if question_rating:
        question_rating = re.sub("[^0-9]", "", str(question_rating))
    else:
        question_rating = "<None>"

    # question_author_name
    if question_author_name:
        question_author_name = cleaning_text(str(question_author_name.text))
    else:
        question_author_name = "<None>"

    # question_author_rank, question_author_rating
    if question_author_rating:
        question_author_rank = re.sub("[^а-яА-Я]", "", str(question_author_rating))
        question_author_rating = re.sub("[^0-9]", "", str(question_author_rating))
    else:
        question_author_rank = "<None>"
        question_author_rating = "<None>"

    # answers

    # answers_dates
    p_answers_dates = []
    for answer_date in answers_dates: p_answers_dates.append(str(answer_date)[75:100])
    answers_dates = p_answers_dates
    del p_answers_dates

    # answers_texts
    p_answers_texts = [] # re.sub(r"\n", "", str(question_head.text))
    for answer_text in answers_texts:
        p_answer_text = cleaning_text(str(answer_text.text))
        p_answers_texts.append(p_answer_text)
    answers_texts = p_answers_texts
    del p_answers_texts

    # answers_ratings
    p_answers_ratings = []
    for answer_rating in answers_ratings: p_answers_ratings.append(re.sub("[^0-9]", "", str(answer_rating)))
    answers_ratings = p_answers_ratings
    del p_answers_ratings

    # answers_author_names
    p_answers_authors_names = []
    for answer_author_name in answers_authors_names:
        p_answer_author_name = cleaning_text(str(answer_author_name.text))
        p_answers_authors_names.append(p_answer_author_name)
    answers_authors_names = p_answers_authors_names
    del p_answers_authors_names

    # answers_author_ratings
    p_answers_authors_ranks = []
    p_answers_authors_ratings = []
    for answer_author_rating in answers_authors_ratings:
        p_answers_authors_ranks.append(re.sub("[^а-яА-Я]", "", str(answer_author_rating)))
        p_answers_authors_ratings.append(re.sub("[^0-9]", "", str(answer_author_rating)))
    answers_authors_ranks, answers_authors_ratings = p_answers_authors_ranks, p_answers_authors_ratings
    del p_answers_authors_ranks, p_answers_authors_ratings

    # create dict with all data

    # append question and base of answer
    question_data = {"question_number": f"{question_number}", "question_category": f"{question_category}",
                     "question_date": f"{question_date}", "question_head": f"{question_head}",
                     "question_text": f"{question_text}", "question_rating": f"{question_rating}",
                     "question_author_name": f"{question_author_name}",
                     "question_author_rank": f"{question_author_rank}",
                     "question_author_rating": f"{question_author_rating}",
                     "answers": {}
                     }

    # append answer data
    for c1, answer_date in enumerate(answers_dates):
        question_data["answers"].update({
            f"answer_{c1+1}": {
                "answer_date": f"{answer_date}",
                "answer_text": f"{answers_texts[c1]}",
                "answer_rating": f"{answers_ratings[c1]}",
                "answer_author_name": f"{answers_authors_names[c1]}",
                "answer_author_rank": f"{answers_authors_ranks[c1]}",
                "answer_author_rating": f"{answers_authors_ratings[c1]}"
            }
        })
    # return data
    # print(question_data)
    return question_data


# write data to jsonl file
async def write_data_to_jsonl(parsed_data: dict, jsonl_file: any):
    await jsonl_file.write(str(json.dumps(parsed_data, ensure_ascii=False)+"\n"))


# parse one page
async def parse_page(page_url: str, number_of_page: int, max_sleep_value: float, jsonl_file: any, headers=None, proxy=None):
    # get html
    html = await get_requests(page_url, headers, proxy)
    # parse html
    parsed_data = await get_data_from_page(html, number_of_page)
    # save data
    await write_data_to_jsonl(parsed_data, jsonl_file)
    # sleep some random time
    await asyncio.sleep(random.uniform(0.0, max_sleep_value))


# create task objects for all urls
async def create_tasks(urls_list: list, numbers_list: list, max_sleep_value: float, jsonl_file: any, headers=None, proxy=None):
    task_list = []
    # iterate urls
    for c1, url in enumerate(urls_list):
        # create task (not started function)
        task = await asyncio.create_task(parse_page(url, numbers_list[c1], max_sleep_value, jsonl_file, headers, proxy), name=f"parsed_page: {c1}")
        # append to list
        task_list.append(task)
    # return list of stak
    return task_list


# parse all task
async def async_tasks_execution(urls_list: list, numbers_list: list, max_sleep_value: float, jsonl_file_name: str, headers=None, proxy=None):
    # open jsonl file for results
    async with aiofiles.open(jsonl_file_name, mode='a', encoding="utf-8") as f:
        # create task list
        task_list = await create_tasks(urls_list, numbers_list, max_sleep_value, f, headers, proxy)
        # execution all task
        done = asyncio.wait(task_list)


# parse part of data
# since there are a lot of pages, their parsing will be divided into many parts
# step: number of pages in one step ; start: number of start page ; end: number of end page ; jsonl_file_name: name of jsonl file ;
# max_sleep_value: max sleep value in random sleep after page parse ; proxy: proxy ; rotate_user_agent: swap user agent avery step ;
# rewrite_jsonl: rewrite json file, if rewrite_jsonl == True ;
def parse_data_on_parts(step: int, start: int, end: int, max_sleep_value: float, jsonl_file_name: str, proxy=None,
                        rotate_user_agent=False, rewrite_jsonl=True):
    # params
    start_time = time.time() # time of parse
    start_of_step = start # number of start page
    end_of_step = start+step # number of end page
    iterate_part = True # iterate while True

    # rewrite json file, if rewrite_jsonl == True
    if rewrite_jsonl:
        # create jsonl file for all data
        f = open(f"{jsonl_file_name}.jsonl", "w", encoding="utf-8")
        f.close()

    # iterate parts
    while iterate_part:
        # try to make step
        try:
            # create pages list
            page_list = []
            numbers_list = []
            for number in range(start_of_step, end_of_step):
                page_url = f"https://otvet.mail.ru/question/{number}"
                page_list.append(page_url)
                numbers_list.append(number)

            # create params for parse
            # headers
            if rotate_user_agent:
                user_agent = UserAgent()
                HEADERS = {'user-agent': f'{user_agent.chrome}',
                           'accept': '*/*',
                           'referer': 'https://www.google.com/'}
            else:
                HEADERS = {'user-agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
                           'accept': '*/*',
                           'referer': 'https://www.google.com/'}

            # start parsing
            # parse
            asyncio.run(async_tasks_execution(page_list, numbers_list, max_sleep_value, f"{jsonl_file_name}.jsonl", headers=HEADERS, proxy=proxy))

        # if error in step
        except:
           logger.info(f"error in process on step {start_of_step}")

        # increase start and end values
        start_of_step += step
        end_of_step += step
        logger.info(f"data in process processed on {int((start_of_step - start) / ((end - start) / 100))}%")

        # if number of processed pages > end
        if start_of_step > end:
            # stop iterations
            iterate_part = False

    # time to all work
    end_time = time.time()
    return end_time - start_time


# params
# proxy = "http://xwTUe3:DZWtQe@195.158.194.23:8000"

# test
global_time = parse_data_on_parts(step=100, start=229000000, end=229001000, max_sleep_value=0.3,
                                  jsonl_file_name="all_mail_parse_part_3", proxy=None, rotate_user_agent=True,
                                  rewrite_jsonl=True)
# 770 sec for 5000 urls
print(global_time)