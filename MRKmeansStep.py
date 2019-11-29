"""
.. module:: MRKmeansDef

MRKmeansDef
*************

:Description: MRKmeansDef

    

:Authors: bejar
    

:Version: 

:Created on: 17/07/2017 7:42 

"""

from collections import defaultdict

from mrjob.job import MRJob
from mrjob.step import MRStep

import os

__author__ = 'bejar'


class MRKmeansStep(MRJob):
    prototypes = {}

    def jaccard(self, prot, doc):
        """
        Compute here the Jaccard similarity between  a prototype and a document
        prot should be a list of pairs (word, probability)
        doc should be a list of words
        Words must be alphabeticaly ordered

        The result should be always a value in the range [0,1]
        """
        doc2 = set(doc)
        doc_product = sum((prob for word, prob in prot if word in doc2))
        l2_doc1 = sum((prob*prob for _,prob in prot))
        l2_doc2 = len(doc)
        result = doc_product / (l2_doc1+l2_doc2-doc_product)
        return result

    def configure_args(self):
        """
        Additional configuration flag to get the prototypes files

        :return:
        """
        super(MRKmeansStep, self).configure_args()
        self.add_file_arg('--prot')

    def load_data(self):
        """
        Loads the current cluster prototypes

        :return:
        """
        f = open(self.options.prot, 'r')
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            self.prototypes[cluster] = cp

    def assign_prototype(self, _, line):
        """
        This is the mapper it should compute the closest prototype to a document

        Words should be sorted alphabetically in the prototypes and the documents

        This function has to return at list of pairs (prototype_id, document words)

        You can add also more elements to the value element, for example the document_id
        """

        # Each line is a string docid:wor1 word2 ... wordn
        doc, words = line.split(':')
        lwords = words.split()
        best_distance = 1
        best_prototype_id = None
        for id_, prototype in self.prototypes.items():
            distance = self.jaccard(prototype, lwords)
            #with open('/home2/users/alumnes/1227294/dades/linux/CAIM/Sessio 6/distances.txt', 'a') as f:
            #    f.write(f'{distance}\n')
            if distance <= best_distance:
                best_distance = distance
                best_prototype_id = id_
        # Return pair key, value
        yield best_prototype_id, list(sorted(lwords))

    def aggregate_prototype(self, key, values):
        """
        input is cluster and all the documents it has assigned
        Outputs should be at least a pair (cluster, new prototype)

        It should receive a list with all the words of the documents assigned for a cluster

        The value for each word has to be the frequency of the word divided by the number
        of documents assigned to the cluster

        Words are ordered alphabetically but you will have to use an efficient structure to
        compute the frequency of each word

        :param key:
        :param values:
        :return:
        """
        word_frequency = defaultdict(int)
        n_words = 0
        for value in values:
            for word in value:
                n_words += 1
                word_frequency[word] += 1
        
        yield key, list(sorted( ((word,frequency/n_words) for word, frequency in word_frequency.items()), key=lambda x: x[0] ))

    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()
