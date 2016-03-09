import sys
import getopt
import csv
import time
import numpy as np
import matplotlib.pyplot as plt
from scipy.sparse import *
from sklearn.cross_validation import cross_val_score
from sklearn import *	
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error

#buildStepDecisionTree(numOfFeatures, numOfTrain, numOfTest, trainPath, testPath, maxDepth):
#main numOfFeatures numOfTrain numOfTest trainPath testPath maxDepth: 
def main():
	print(sys.argv[1:])
	#opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
	#print(args)
	#numOfTrain = argv[0];
	numOfFeatures = int(sys.argv[1])
	numOfTrain = int(sys.argv[2])
	numOfTest = int(sys.argv[3])
	trainPath = sys.argv[4]
	testPath = sys.argv[5]
	maxDepth = int(sys.argv[6])
	iteration = int(sys.argv[7])
	rows = numOfTrain
	columns = numOfFeatures
	print((rows + columns))
	matrix = lil_matrix((rows, columns))

	print(matrix.shape)

	csvreader = csv.reader(open(trainPath), delimiter=',')
	#csvreader = csv.reader(open('a.csv'))
	targets = []
	row = 0;
	for line in csvreader:
		#print(line)
		if line[0] == '1':
			#matrix.data[row].append(0)
			targets.append(1)
		else:
			targets.append(0)
		for index in range(len(line)):
			if index > 0:
				matrix[row,int(line[index])-1] = 1
	#			matrix.data[row].append(int(line[index]))
		row = row + 1;
		#print(row)
	print(matrix.shape)
	print(len(targets))
	#=========================================================================================

	from sklearn import metrics
	from sklearn.tree import DecisionTreeRegressor
	# fit a CART model to the data
	#model = DecisionTreeRegressor()
	if maxDepth == -1:
		regressor = DecisionTreeRegressor()
	else:
		regressor = DecisionTreeRegressor(max_depth=maxDepth)
	now = time.time()
	regressor.fit(matrix, targets)
	later = time.time()
	difference = int(later - now)
	print(regressor)
	#=====================================================================================
	# Visualize the tree
	treeStructWriter= open('RegTree/treeStruct_'+str(iteration)+'.txt', 'w');
	n_nodes = regressor.tree_.node_count
	children_left = regressor.tree_.children_left
	children_right = regressor.tree_.children_right
	feature = regressor.tree_.feature
	threshold = regressor.tree_.threshold
	values = regressor.tree_.value
	print(len(feature))
	print(feature)
	print(zip(matrix[:,regressor.tree_.feature], regressor.tree_.threshold, regressor.tree_.children_left,regressor.tree_.children_right))
	# The tree structure can be traversed to compute various properties such
	# as the depth of each node and whether or not it is a leaf.
	node_depth = np.zeros(shape=n_nodes)
	is_leaves = np.zeros(shape=n_nodes, dtype=bool)
	stack = [(0, -1)]  # seed is the root node id and its parent depth
	while len(stack) > 0:
		node_id, parent_depth = stack.pop()
		node_depth[node_id] = parent_depth + 1

		# If we have a test node
		if (children_left[node_id] != children_right[node_id]):
			stack.append((children_left[node_id], parent_depth + 1))
			stack.append((children_right[node_id], parent_depth + 1))
		else:
			is_leaves[node_id] = True

	print("The binary tree structure has %s nodes and has "
		  "the following tree structure:"
		  % n_nodes)
	for i in range(n_nodes):
		if is_leaves[i]:
			treeStructWriter.write("N%s leafNode = %ss " % (i, values[i]))
			treeStructWriter.write("\n")
#			print("%snode=%s leaf node." % (int(node_depth[i]) * "\t", i))
		else:
			treeStructWriter.write("N%s X_%s <= %ss then N%s else N%s" % (i, feature[i], threshold[i], children_left[i], children_right[i]))
			treeStructWriter.write("\n")
#			print("%snode=%s test node: go to node %s if X[:, %s] <= %ss else to "
#				  "node %s." % (int(node_depth[i]) * "\t", i, children_left[i], feature[i], threshold[i], children_right[i], ))
	print()
	treeStructWriter.close()
	#=========================================================================================
	rows = numOfTest
	columns = numOfFeatures
	X = lil_matrix( (rows, columns) )
	print(X.shape)
	csvreader = csv.reader(open(testPath), delimiter=',')
	predictionWriter= open('RegTree/predictions_'+str(iteration)+'.txt', 'w');

	targets_test = []
	row = 0;
	for line in csvreader:
		#print(line)
		if line[0] == '1':
			targets_test.append(1)
		else:
			targets_test.append(0)
		for index in range(len(line)):
			if index > 0:
				X[row,int(line[index])-1] = 1
		row = row + 1;
		#print(row)
	print(X.shape)
	print(len(targets_test))

	expected = targets_test
	predicted = regressor.predict(X)
#	error = 0;
	print(len(predicted))
	for i in range(len(predicted)):
		predictionWriter.write(str(expected[i])+" "+str(predicted[i]))
		predictionWriter.write("\n")
#		error = error + abs(expected[i] - predicted[i])
#		if( abs(expected[i] - predicted[i]) > 0):
#			print (str(expected[i]) + " - " + str(predicted[i]))
#		print (str(expected[i]) + " - " + str(predicted2[i]) + " - " + str(predicted5[i]) + " - " + str(predicted[i]))
	predictionWriter.close();
	print("error: " + str(mean_squared_error(expected,predicted)))
	print("r2_score: " + str(r2_score(expected,predicted)))
#	print("cross_val_score: " + str(cross_val_score(expected,predicted)))
	dotfile = open("dtree1.dot", 'w')
	tree.export_graphviz(regressor, out_file = dotfile)
	dotfile.close()
	return predicted
if __name__ == "__main__":
    main()
#trainPath = '/Users/zahraiman/University/FriendSensor/SPARK/SocialSensorProject_Jan28/socialsensor/Data/test/Learning/Topics/naturaldisaster/fold1166582/testTrain_train_.arff'
#testPath = '/Users/zahraiman/University/FriendSensor/SPARK/SocialSensorProject_Jan28/socialsensor/Data/test/Learning/Topics/naturaldisaster/fold1166582/testTrain_test_.arff'
#numOfTrain = 376
#numOfTest = 1624
#numOfFeatures = 932
#predicted = buildStepDecisionTree(numOfFeatures, numOfTrain, numOfTest, trainPath, testPath, -1)
#print(len(predicted))