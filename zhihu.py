import zhihu_oauth
from zhihu_oauth.zhcls import SearchType
from zhihu_oauth import GetDataErrorException
import os
import time
import pymysql
import pymysql.cursors
import csv
import pandas as pd

class Pku_Zhihu(object):
    def __init__(self):
        self.client = zhihu_oauth.ZhihuClient()
        self.TokenFile = "token.pkl"
        self.connect = None
        self.cursor = None
        self.author_num = 0
        self.question_num = 0
        self.answer_count = 0

    def login(self):
        if os.path.isfile(self.TokenFile):
            self.client.load_token(self.TokenFile)
        else:
            self.client.login_in_terminal()
            self.client.save_token(self.TokenFile)

    def connectDatabase(self):
        try:
            self.connect = pymysql.connect(host='localhost', user='root', passwd='123456', db='俄罗斯', port=3306, charset='utf8')
            self.cursor = self.connect.cursor()
        except Exception as e:
            print ("连接数据库异常")

    def closeDatabase(self):
        self.cursor.close()
        self.connect.close()

    def clearTable(self):
        self.connectDatabase()
        try:
            self.cursor.execute("truncate table authors;")
            self.cursor.execute("truncate table questions;")
            for i in range(10):
                answer_table_name = "answers" + str(i)
                self.cursor.execute("truncate table %s;" % answer_table_name)
            self.connect.commit()
        except Exception:
            print ("清空表出错")
        self.closeDatabase()

    def crawlQuestion(self, question):
        #self.showResult()
        self.connectDatabase()
        #问题信息
        try:
            question_id = question.id
            title = question.title
            question_describe = question.detail
            updated_time = zhihu_oauth.ts2str(question.updated_time)
            answer_count = question.answer_count
            follower_count = question.follower_count
        except GetDataErrorException as e:
            #停止爬该问题，继续下一个问题。
            print ("获取问题失败")
            print (e.reason)
            return

        #将问题插入问题表
        print ("正在爬取问题：" + title)
        print ("共" + str(answer_count) + "个回答")
        try:
            sql = "insert into questions values(%d, '%s', '%s', '%s', %d, %d);" % (question_id, title, question_describe, updated_time, answer_count, follower_count)
            #print (sql)
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception:
            print ("插入问题表出错")
            #print (e.with_traceback())
            #self.closeDatabase()
            return
        else:
            self.question_num += 1
            print ("问题成功写入问题表")

        num = 0
        for answer in question.answers:
            # time.sleep(1)
            num += 1
            # if (num % 30 == 0):
            #     time.sleep(10)
            print("正在爬取第" + str(num) + "个回答：")
            #作者信息
            author_id = answer.author.id
            if ((author_id != 0) & (author_id != "0")):
                try:
                    author_name = answer.author.name
                except GetDataErrorException as e:
                    print ("获取用户名出错")
                    print (e.reason)
                    author_name = ""
                try:
                    gender = answer.author.gender
                    if gender == None:
                        gender = -1
                except GetDataErrorException as e:
                    print ("获取用户性别出错")
                    print (e.reason)
                    gender = -1
                try:
                    headline = answer.author.headline
                except GetDataErrorException as e:
                    print ("获取用户个性签名出错")
                    print (e.reason)
                    headline = ""
                try:
                    follower_count = answer.author.follower_count
                    thanked_count = answer.author.thanked_count
                    voteup_count = answer.author.voteup_count
                except GetDataErrorException as e:
                    print ("获取用户粉丝数等信息出错")
                    print (e.reason)
                    follower_count = -1
                    thanked_count = -1
                    voteup_count = -1
                try:
                    sql = "insert into authors values('%s', '%s', %d, '%s', %d, %d, %d);" % (author_id, author_name, gender, headline, follower_count, thanked_count, voteup_count)
                    print (sql)
                    self.cursor.execute(sql)
                    self.connect.commit()
                except Exception:
                    print ("插入作者表失败")
                    #self.closeDatabase()
                    #continue
                else:
                    print ("作者信息成功写入作者表")
                    self.author_num += 1
            #回答信息
            answer_id = answer.id
            try:
                content = answer.content
            except GetDataErrorException as e:
                content = ""
                print ("获取回答内容失败")
                print (e.reason)
            try:
                updated_time = zhihu_oauth.ts2str(answer.updated_time)
            except GetDataErrorException as e:
                print (e.reason)
                updated_time = zhihu_oauth.ts2str(0)
            try:
                voteup_count = answer.voteup_count
                thanks_count = answer.thanks_count
            except GetDataErrorException as e:
                print ("获取回答赞同数等信息失败")
                print (e.reason)
                voteup_count = -1
                thanks_count = -1

            content = content.replace("'", "''")

            answer_table_name = "answers" + str(answer_id % 9)


            # print (content)
            try:
                sql = "insert into %s values(%d, %d, '%s', '%s', '%s', %d, %d);" % (answer_table_name, answer_id, question_id, author_id, content, updated_time, voteup_count, thanks_count)
                #print (sql)
                self.cursor.execute(sql)
                self.connect.commit()
            except:
                print ("插入回答表出错:" + str(num))
                #self.closeDatabase()
                #continue
            else:
                print ("爬取第" + str(num) + "个回答成功")
                self.answer_count += 1

        if (self.answer_count % 30 == 0):
            time.sleep(1)
        self.closeDatabase()

    def crawlTopic(self, topic):

        print ("正在爬取话题： " + topic.name)
        print ("共" + str(topic.questions_count) + "个问题……")
        num = 1
        for question in topic.unanswered_questions:
            print("-------------------------------")
            print ("进行到第" + str(num) + "个问题：")
            #time.sleep(1)
            self.crawlQuestion(question)
            num += 1

    def test(self, query):
        filenames = os.listdir("俄罗斯话题")
        for result in self.client.search(query, SearchType.TOPIC):

            topic = result.obj
            if topic.name + ".csv" in filenames:
                print ("已经存在：" + topic.name)
                continue
            time.sleep(1)
            print (topic.questions_count)
            with open(topic.name + ".csv", "w", newline="") as f:
                print ("正在写话题" + topic.name)
                writer = csv.writer(f)
                writer.writerow(["问题ID", "回答数"])
                num = 0
                for question in topic.unanswered_questions:
                    num += 1
                    if(num % 30 == 0):
                        time.sleep(1)
                        print ("正在写第" + str(num) + "行")
                    writer.writerow([question.id, question.answer_count])

    def crawlByIndex(self):
        self.clearTable()
        topicfile_list = os.listdir("俄罗斯话题")
        for topicfile in topicfile_list:
            questions = []
            print (topicfile)
            #获取当前话题的问题ID列表
            print ("正在爬取话题文件" + topicfile)
            with open(os.path.join("俄罗斯话题", topicfile)) as f:
                f.readline()
                num = 0
                for line in f.readlines():
                    q_id = int(line.strip().split(",")[0])
                    questions.append(q_id)
                    num += 1
                print (str(num) + "个问题")
            num = 0
            for q_id in questions:
                num += 1
                print ("第" + str(num) + "个问题")
                self.crawlQuestion(self.client.question(q_id))

            time.sleep(5)





    def crawlByQuery(self, query):
        self.clearTable()
        num = 0

        for result in self.client.search(query, SearchType.TOPIC):

            print("***********************************************************")
            print ("进行到第" + str(num) + "个话题")
            topic = result.obj
            self.crawlTopic(topic)
            time.sleep(5)

    def showResult(self):
        print ("----共爬取了：")
        print (str(self.question_num) + "个问题")
        print (str(self.author_num) + "个作者")
        print (str(self.answer_count) + "个回答")


if __name__ == "__main__":
    zhihu = Pku_Zhihu()
    zhihu.login()
    zhihu.crawlByIndex()
    #zhihu.test("俄罗斯")

    '''
    try:
        zhihu.crawl("俄罗斯")
    except Exception as e:
        print (e.with_traceback())
    finally:
        zhihu.showResult()
    '''