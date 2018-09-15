from pyspark import SparkContext
from collections import OrderedDict
import sys
import itertools
import copy
import networkx as nx
from networkx.algorithms.community import girvan_newman
import time

def get_movie_rating_counts(iterator):
	movies = dict(list(iterator))
	for index, movie in enumerate(movies):
		user_list = sorted(movies[movie])
		
		for user_pair in itertools.combinations(user_list, 2):
			yield (user_pair, 1)

def generate_adj_matrix(iterator):
	iterator = list(iterator)
	yield (iterator[0], iterator[1])
	yield (iterator[1], iterator[0])

if __name__ == "__main__":
	ratings_file = sys.argv[1]
	sc = SparkContext(appName = "Girvan Newman")
	ml_latest_small = sc.textFile(ratings_file)
	header = ml_latest_small.first()
	ml_latest_small = ml_latest_small.filter(lambda x : x != header)
	ml_latest_small = ml_latest_small\
	.map(lambda x : reversed([int(i) for i in x.split(",")[:2]]))\
	.groupByKey()\
	.mapValues(lambda value : [v for v in value])

	adj_matrix = ml_latest_small\
	.mapPartitions(get_movie_rating_counts)\
	.groupByKey()\
	.mapValues(lambda value_list : len(value_list))\
	.filter(lambda x : x[1] >= 9)\
	.map(lambda x : x[0])\
	.map(generate_adj_matrix)\
	.flatMap(lambda x:x)\
	.groupByKey()\
	.map(lambda x : (x[0], set(x[1])))\
	.collect()

	dict_adj_matrix = dict(adj_matrix)

	graph = nx.from_dict_of_lists(dict_adj_matrix)

	genenrated_communities = girvan_newman(graph)

	start_time = time.time()

	while(time.time() - start_time < 200):
		communities = [sorted(i) for i in next(genenrated_communities)]

	with open("Vishal_Seshagiri_Community_Bonus.txt", "w") as file:
		communities = sorted(communities, key=lambda x : x[0])
		for community in communities:
			file.write("[{}]\n".format(",".join([str(j) for j in community]).strip(",")))
