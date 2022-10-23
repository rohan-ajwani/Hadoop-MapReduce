from mrjob.job import MRJob
from kmeans_mrjob import MRKMeans
import sys,os
import os.path
import shutil
import time

input_c = "centroids.txt"

Centroids_File = "centroids_7.txt"

def get_centroids(job, runner):
    c = []    
    #for line in runner.stream_output():
    for key, value in job.parse_output(runner.cat_output()):
        c.append(value)
    return c

def get_first_centroids(fname):
    centroids = []
    with open(fname, 'r') as f:        
        for line in f:
            if line:
                centroid = line.split(',')
                centroids.append([float(centroid[0]), float(centroid[1])])
    return centroids

def write_c(centroids):
    with open(Centroids_File,'w') as f:
        centroids.sort()
        for centroid in centroids:
            k, x, y = centroid.split(',')
            f.write("%s,%s\n"%(x, y))


if __name__ == '__main__':

    args = sys.argv[1:]

    os.remove(Centroids_File)
    shutil.copy(input_c, Centroids_File)

    old_c = get_first_centroids(input_c)

    orig_start_time = time.time()
    for i in range(15):
        start_time = time.time()
        print("Iteration %i" % (i+1))
        mr_job = MRKMeans(args=args + ['--centroids='+Centroids_File])        
        with mr_job.make_runner() as runner:
            runner.run()
            centroids = get_centroids(mr_job,runner)
            write_c(centroids)
        n_c = get_first_centroids(Centroids_File)
        for c in n_c:
            print(c[0],c[1])
        print("Time Taken for this iteration = %s seconds" % (time.time() - start_time))
    print("Total Time taken for 15 iterations = %s seconds" % (time.time() - orig_start_time))
