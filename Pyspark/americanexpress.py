"𝐏𝐫𝐨𝐛𝐥𝐞𝐦 𝐒𝐭𝐚𝐭𝐞𝐦𝐞𝐧𝐭 : Determine the user_id and corresponding page_id of the pages liked by their friend but not by the user itself yet."
#Create DataFrame code :
##=================
friends_data = [(1, 2),
(1, 3),
(1, 4),
(2, 1),
(3, 1),
(3, 4),
(4, 1),
(4, 3)]

friend_schema = "user_id int , friend_id int"
friends_df = spark.createDataFrame(data =friends_data , schema = friend_schema )

likes_data = [
(1, 'A'),
(1, 'B'),
(1, 'C'),
(2, 'A'),
(3, 'B'),
(3, 'C'),
(4, 'B')
]

like_schema = "user_id int , page_id string"
likes_df = spark.createDataFrame(data =likes_data , schema = like_schema )

display(friends_df)
display(likes_df)

#joining friends and the likes dataframe. This gives the pages liked by the each user.
friend_like_df = friends_df.join(likes_df, on 'user_id', how = "inner")
#creating concat of friend and page id's
friend_page_concat_df = friend_like_df.withColumn("user_page", concat(col("friend_id"), col("page_id")))
#creating the friend_liked list
friend_liked [row.friend_page for row in likes_conact_df.collect()]
# finding the userid and the corresponding pages liked by the their friend by not by the user.
friend_page_concat_df = friend_page_concat_df.filter(~col('user_page').isin (friend_liked))
# Finding the unique records.
answer_df = friend_page_concat_df.select(col("friend_id").alias("user_id"), col("page_id")).distinct()
#Order the record.
answer_df answer_df.orderBy("user_id")
display (answer_df)

#####Another approach
df1 = friends_df.join(likes_df,friends_df.friend_id == likes_df.user_id,'left').drop(likes_df.user_id)
df2 = df1.select('user_id','page_id').distinct()
df3 = df1.select(col('friend_id').alias('user_id'),'page_id').distinct()
res = df2.subtract(df3)
res.display()