from mrjob.job import MRJob
import math

class MRKMeans(MRJob):

    def configure_args(self):
        super(MRKMeans, self).configure_args()
        self.add_file_arg('--centroids')

    def getCentroids(self):
        centroids = []

        with open(self.options.centroids,'r') as f:
            points = f.readlines()
            for point in points:
                point = point.strip()
                point = point.split(',')
                centroids.append([float(point[0]), float(point[1])])
        
        return centroids


    def getDistance(self, p1, p2):
    #returns Euclidean Distance
    #p1 and p2 are lists with 2 elements, ie coordinates
        distance = math.sqrt(math.pow((p1[0]-p2[0]),2) + math.pow((p1[1]-p2[1]),2))
        return distance


    def mapper(self, _,lines):

        centroids = self.getCentroids()
        num_centroids = len(centroids)

        for line in lines.split('\n'):

            assigned_centroid_id = -1
            line = line.strip()
            point = line.split(',')
            point = [float(point[0]), float(point[1])]
            min_dist = math.inf

            for i in range(num_centroids):
                dist = self.getDistance(centroids[i],point)
                if (min_dist > dist):
                    min_dist = dist
                    assigned_centroid_id = i

            yield assigned_centroid_id, point #gives centroid id as key and point = [x,y] as value

    def reducer(self, key, values):
        """
        Definition : for each class, get all the tmp centroids from each combiner and calculate the new centroids.
        """
        # k is class and v are medium points linked to the class
        current_centroid = -1
        sum_x = 0
        sum_y = 0
        num_points = 0

        for v in values:
            num_points += 1
            sum_x += v[0]
            sum_y += v[1]

        yield (None, str(key)+","+str(sum_x/num_points)+","+str(sum_y/num_points))

if __name__ == '__main__':
    MRKMeans.run()
