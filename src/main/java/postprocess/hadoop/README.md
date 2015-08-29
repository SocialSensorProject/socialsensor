Creator: Reda

This code implements the MAP/Reduce functions for the five tasks. In fact, each task is implemented in a separates class. For example, the task that computes the birthday of each hashtag is implemented in the class: eecs.oregonstate.edu.tweets.HashtagBirthday. Note that each class output a tab-separated values file, which can be directly imported in MySQL using a command like this: 


LOAD DATA local INFILE 'HashtagBirthday.txt' INTO TABLE HashtagBirthday FIELDS TERMINATED BY '\t';
