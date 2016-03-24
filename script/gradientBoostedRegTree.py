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
from sklearn import metrics
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import GradientBoostingClassifier

#main numOfFeatures numOfTrain numOfTest trainPath testPath maxDepth: 
def main():
	print(sys.argv[1:])
	numOfFeatures = int(sys.argv[1])
	numOfTrain = int(sys.argv[2])
	numOfTest = int(sys.argv[3])
	trainPath = sys.argv[4]
	testPath = sys.argv[5]
	maxDepth = int(sys.argv[6])
	iteration = int(sys.argv[7])
	enableRegressor = int(sys.argv[8])
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

	# fit a CART model to the data
	#model = DecisionTreeRegressor()
	if enableRegressor == 1:
		regressor = GradientBoostingClassifier(loss='deviance',learning_rate=0.1, n_estimators=100, subsample=1.0, min_samples_split=2, min_samples_leaf=1, min_weight_fraction_leaf=0.0, max_depth=maxDepth, init=None, random_state=None, max_features=None, verbose=0, max_leaf_nodes=None)
	else:
		regressor = GradientBoostingRegressor(loss='ls',earning_rate=0.1, n_estimators=100, subsample=1.0, min_samples_split=2, min_samples_leaf=1, min_weight_fraction_leaf=0.0, max_depth=maxDepth, init=None, random_state=None, max_features=None, alpha=0.9, verbose=0, max_leaf_nodes=None)
	now = time.time()
	matrix = matrix.toarray();
	print(matrix.shape)
	regressor.fit(matrix, targets)
	later = time.time()
	difference = int(later - now)
	print(regressor)
	#=====================================================================================
	#=========================================================================================
	rows = numOfTest
	columns = numOfFeatures
	X = lil_matrix( (rows, columns) )
	print(X.shape)
	csvreader = csv.reader(open(testPath), delimiter=',')

	targets_test = []
	row = 0;
	for line in csvreader:
		if line[0] == '1':
			targets_test.append(1)
		else:
			targets_test.append(0)
		for index in range(len(line)):
			if index > 0:
				X[row,int(line[index])-1] = 1
		row = row + 1;
	print(len(targets_test))
	expected = targets_test
	X = X.toarray()
	print(X.shape)
	predicted = regressor.predict(X)
	predictionWriter= open('RegTree/predictions_gradientBoosted_'+str(iteration)+'.txt', 'w');
	for i in range(len(predicted)):
		predictionWriter.write(str(expected[i])+" "+str(predicted[i]))
		predictionWriter.write("\n")
	predictionWriter.close()
	print("error: " + str(mean_squared_error(expected,predicted)))
	print("r2_score: " + str(r2_score(expected,predicted)))
#	print("cross_val_score: " + str(cross_val_score(expected,predicted)))
	return predicted
if __name__ == "__main__":
    main()
