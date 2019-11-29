"""
.. module:: MRKmeans

MRKmeans
*************

:Description: MRKmeans

    Iterates the MRKmeansStep script

:Authors: bejar
    

:Version: 

:Created on: 17/07/2017 10:16 

"""

from MRKmeansStep import MRKmeansStep
import shutil
import argparse
import os
import time
from mrjob.util import to_lines
from math import sqrt

__author__ = 'bejar'

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prot', default='prototypes.txt', help='Initial prototypes file')
    parser.add_argument('--docs', default='documents.txt', help='Documents data')
    parser.add_argument('--iter', default=100, type=int, help='Number of iterations')
    parser.add_argument('--ncores', default=2, type=int, help='Number of parallel processes to use')

    args = parser.parse_args()
    assign = {}

    # Copies the initial prototypes
    cwd = os.getcwd()
    shutil.copy(cwd + '/' + args.prot, cwd + '/prototypes0.txt')
    
    epsilon = 1e-2
    old_prototypes = dict()
    with open(cwd + '/' + args.prot, 'r') as f:
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            old_prototypes[cluster] = dict(cp)

    for i in range(args.iter):
        tinit = time.time()  # For timing the iterations

        # Configures the script
        print('Iteration %d ...' % (i + 1))
        # The --file flag tells to MRjob to copy the file to HADOOP
        # The --prot flag tells to MRKmeansStep where to load the prototypes from
        doc_num = i%2
        mr_job1 = MRKmeansStep(args=['-r', 'local', args.docs,
                                     '--file', cwd + '/prototypes%d.txt' % doc_num,
                                     '--prot', cwd + '/prototypes%d.txt' % doc_num,
                                     '--num-cores', str(args.ncores)])

        # Runs the script
        new_prototypes = dict()
        with mr_job1.make_runner() as runner1:
            runner1.run()
            with open(f'{cwd}/prototypes{(doc_num+1) % 2}.txt', 'w') as proto_file:
                # Process the results of the script iterating the (key,value) pairs
                for prototype, word_vector in mr_job1.parse_output(runner1.cat_output()):
                    new_prototypes[prototype] = dict(word_vector)
                    proto_file.write(f'{prototype}:')
                    for word, weight in word_vector:
                        proto_file.write(f'{word}+{weight} ')
                    proto_file.write('\n')
                    
        # Average L2 between prototypes word_vecs
        sum_l2 = 0
        n_clusters = len(new_prototypes)
        for cluster, word_vec in new_prototypes.items():
            if cluster not in old_prototypes:
                break
            l2 = 0
            for word, weight in word_vec.items():
                old_weight = old_prototypes[cluster].get(word, 0)
                diff = old_weight - weight
                l2 += diff * diff
            sum_l2 += sqrt(l2)                    
                    
        else:
            if sum_l2 < epsilon*n_clusters:
                print(f'Prototypes in file prototypes{i%2}.txt')
                break
        
        print(f'Average L2 {sum_l2/n_clusters}')
        
        old_prototypes = new_prototypes
        print(f"Time= {(time.time() - tinit)} seconds")
    # Now the last prototype file should have the results
