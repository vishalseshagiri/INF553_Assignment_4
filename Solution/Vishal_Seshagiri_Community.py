from pyspark import SparkContext
from collections import OrderedDict
import sys
import itertools
import copy

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


def bfs_new(node, dict_adj_matrix):
	visited_order_set, visit_order, seen, to_visit, to_visit_set = set(), list(), dict(), [node], set()
	to_visit_set.add(node)
	
	while(to_visit):
		vertex = to_visit.pop(0)
		to_visit_set.remove(vertex)
		
		if vertex == node:
			seen[node] = [[], 0]
			visit_order.append(node)
			visited_order_set.add(node)
		else:
			visit_order.append(vertex)
			visited_order_set.add(vertex)
				
		vertex_distance_from_root = seen[vertex][1]
		
		### add predecessors in seen
		### filtering out edges which have been already visited
		to_be_appended = dict_adj_matrix[vertex] - visited_order_set
		
		### to_be_appended - to_visit_set
		### to eliminte nodes which are already queued to be visited
		for n in to_be_appended - to_visit_set:
			to_visit_set.add(n)
			to_visit.append(n)
				
				
		for item in to_be_appended:
			if not seen.get(item):
				seen[item] = [[vertex], seen[vertex][1]+1]
			else:
				if seen[item][1] == vertex_distance_from_root + 1:
					seen[item][0].append(vertex)
									
	### addition (calculating the betweenness of the edges in the bfs_new function)
	temp_nodes = {}
	temp_edges = {}
	for inner_node in reversed(visit_order):
		inner_node_value = temp_nodes.get(inner_node)
		temp_nodes[inner_node] = inner_node_value+1 if inner_node_value else 1
		ends = seen[inner_node][0]
		current_inner_node_value = temp_nodes[inner_node]
		number_of_influenced_nodes = len(ends)
		if number_of_influenced_nodes:
			influencing_value = float(current_inner_node_value) / number_of_influenced_nodes
		for end in ends:
			end_node_value = temp_nodes.get(end)
			sorted_edge_list = sorted([inner_node, end])
			edge_name = "{} {}".format(sorted_edge_list[0], sorted_edge_list[1])
			temp_edges[edge_name] = influencing_value
			temp_nodes[end] = end_node_value+influencing_value if end_node_value else influencing_value
					
	#return visit_order
	return temp_edges

def girvan_newman_new(iterator):
	node = iterator
	global dict_adj_matrix
	
	return bfs_new(node, dict_adj_matrix).items()

def bfs_community(communities_list, edge_to_remove):
	global number_of_edges # m
	global dict_adj_matrix
	global new_adj_matrix
	global modularity_community_dict
	
	
	# check if new community is being formed
	nodes_ = edge_to_remove[0]
	set_nodes_ = set(nodes_)
	new_adj_matrix[nodes_[0]] -= set([nodes_[1]])
	new_adj_matrix[nodes_[1]] -= set([nodes_[0]])
	
	# optimization 1
	smaller, to_find = (new_adj_matrix[nodes_[0]], nodes_[1]) if (len(new_adj_matrix[nodes_[0]]) > len(new_adj_matrix[nodes_[1]])) else (new_adj_matrix[nodes_[1]], nodes_[0])
	
	for i in smaller:
		if to_find in new_adj_matrix[i]:
			return communities_list, len(communities_list), None
					
	
	# check if the nodes_[0] exists in the bfs tree of nodes_[1]
	visited_, visited_set, to_visit, to_visit_set = [], set(), [nodes_[1]], set([nodes_[1]])
	
	while(to_visit):
		vertex = to_visit.pop(0)
		to_visit_set.remove(vertex)
		
		visited_.append(vertex)
		visited_set.add(vertex)
		
		to_be_appended = new_adj_matrix[vertex] - visited_set
		
		for n in to_be_appended - to_visit_set:
			to_visit_set.add(n)
			to_visit.append(n)
					
					
	if nodes_[0] in visited_set:
		# girvan newman did not yield any new communities
		return communities_list, len(communities_list), None
	
	else:
		new_communities_list = []
		
		break_point = -1
		for j, community in enumerate(communities_list):
			if break_point==-1 and set_nodes_.issubset(community):
				break_point = j
				
				postponed_mods = []
				two_communities = [visited_set, community - visited_set]
				new_communities_list.extend(two_communities)
				for p_community in two_communities:
					# apply modularity within the community
					comm_modularity = 0
					combinations = list(itertools.combinations(p_community, 2))
					for node_pair in combinations:
						a_ij = 1 if node_pair[0] in dict_adj_matrix[node_pair[1]] else 0
						ki = len(dict_adj_matrix[node_pair[0]])
						kj = len(dict_adj_matrix[node_pair[1]])
						comm_modularity += a_ij*2.0 - (float(ki*kj)/(2*number_of_edges)) * 2.0

					postponed_mods.append(comm_modularity)
									
			else:
				new_communities_list.append(community)
				if break_point != -1:
					if j == break_point + 1:
						temp = modularity_community_dict.get(j)
					else:
						temp_temp = modularity_community_dict.get(j)
						modularity_community_dict[j] = temp
						temp = temp_temp
					# Gross error
					if j == len(communities_list) - 1 and j != break_point:
						modularity_community_dict[j + 1] = temp
				
		if break_point == -1:
			return communities_list, len(communities_list), None
								
		modularity_community_dict[break_point] = postponed_mods[0]
		modularity_community_dict[break_point+1] = postponed_mods[1]
		return_modularity = float(sum(modularity_community_dict.values())) / (2*number_of_edges)

		return new_communities_list, len(new_communities_list), return_modularity


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

	nodes_test = dict_adj_matrix.keys()

	parallelized_nodes = sc.parallelize(nodes_test, 10)

	output_list_of_tuples = parallelized_nodes\
	.flatMap(girvan_newman_new)\
	.groupByKey()\
	.mapValues(lambda x:sum(x)/2)\
	.map(lambda x : (tuple([int(i) for i in x[0].split(" ")]), x[1]))\
	.sortByKey()\
	.collect()

	edges_sorted_by_betweenness = sorted(output_list_of_tuples, key=lambda x : x[1], reverse=True)


	modularity_community_dict = {}
	communities_list = [set(dict_adj_matrix.keys())]
	number_of_communitites = 0
	number_of_edges = len(edges_sorted_by_betweenness)
	modularity_dict = {}
	max_modularity = -1 * float("inf")
	max_modularity_communitites = []
	# dict_adj_matrix = copy.deepcopy(copy_dict)
	new_adj_matrix = copy.deepcopy(dict_adj_matrix)
	count = 0
	for edge in edges_sorted_by_betweenness:
		communities_list, number_of_communitites, modularity_value = bfs_community(communities_list, edge)
		if modularity_value:
			count += 1
			modularity_dict[edge] = modularity_value
			if max_modularity < modularity_value:
				max_modularity = modularity_value
				max_modularity_communitites = communities_list

	with open("Vishal_Seshagiri_Community.txt", "w") as file:
		sorted_max_modularity_communitites = []
		for index, c in enumerate(max_modularity_communitites):
			sorted_max_modularity_communitites.append(sorted(list(max_modularity_communitites[index])))

		sorted_max_modularity_communitites = sorted(sorted_max_modularity_communitites, key=lambda x : x[0])

		for i in sorted_max_modularity_communitites:
			file.write("[{}]\n".format(",".join([str(j) for j in i]).strip(",")))