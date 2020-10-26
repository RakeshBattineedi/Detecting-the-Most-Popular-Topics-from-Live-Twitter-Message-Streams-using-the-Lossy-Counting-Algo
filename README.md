# Detecting-the-Most-Popular-Topics-from-Live-Twitter-Message-Streams-using-the-Lossy-Counting-Algorithm 

Objectives

The goal of this project assignment is to gain experience in:

• Implementing approximate on-line algorithms using a real-time streaming data processing framework

• Understanding and implementing parallelism over a real-time streaming data processing framework

1.Overview
In this assignment, you will design and implement a real-time streaming data analytics system using Apache Storm. The goal of your system is to detect the most frequently
occurring hash tags from the live Twitter data stream in real-time. A hashtag is a type of label or metadata tag used in social networks that makes it easier for
users to find messages with a specific theme or content1 . Users create and use hashtags by placing the hash character (or number sign) # in front of a word or un-spaced phrase.
Searching for that hashtag will then present each message that has been tagged with it. For example, #springbreak and #zidane were popular tags for the US on March 11, 2019.
Finding popular and trendy topics (via hashtags and named entities) in real-time marketing implies that you include something topical in your social media posts to help increase the overall reach. In this assignment, we will target data from live Twitter message provided by Twitter developers2.

In this project, 

• Implement the Lossy Counting algorithm3

• List the top 100 most popular hashtags every 10 seconds

• Parallelize the analysis of your system

To perform above tasks, we require to use Apache Storm, and Twitter Stream APIs.
