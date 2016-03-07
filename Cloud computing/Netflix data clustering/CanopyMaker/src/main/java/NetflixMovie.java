import java.util.*;
public class NetflixMovie {

        public int movie_id;
				public Set<Integer> ids;
				
        public NetflixMovie(int id, String data) {
                this.movie_id = id;
                String[] toAdd = data.split("\\|");
								this.ids = new TreeSet<Integer>();
                for(String s: toAdd){
									String[] data1 = s.split(":");
									this.ids.add(Integer.parseInt(data1[0]));
								}
        }
				
        public int MatchCount(NetflixMovie movie) {
                Iterator<Integer> it1 = this.ids.iterator();
                Iterator<Integer> it2 = movie.ids.iterator();
                int matchCount = 0;
                Integer one = null;
                Integer two = null;
                while((it1.hasNext())&&(it2.hasNext())) {
                        if((one==null)&&(two==null)) {
                                one = it1.next();
                                two = it2.next();
                        }
                        if(one.equals(two)) {
                                matchCount += 1;
                                one = it1.next();
                                two = it2.next();
                        } else if(one.compareTo(two)<0)
                                one = it1.next();
                        else
                                two = it2.next();
                }
                return matchCount;
        }

       
      
        public double SimpleDistance(NetflixMovie movie) {
                Iterator<Integer> it1 = this.ids.iterator();
                Iterator<Integer> it2 = movie.ids.iterator();
                int matchCount = 0;
                int totalCount = 1;
                Integer one = null;
                Integer two = null;
                while((it1.hasNext())&&(it2.hasNext())) {
                        if((one==null)&&(two==null)) {
                                one = it1.next();
                                two = it2.next();
                        }
                        if(one.equals(two)) {
                                matchCount += 1;
                                one = it1.next();
                                two = it2.next();
                        } else if(one.compareTo(two)<0)
                                one = it1.next();
                        else
                                two = it2.next();
                        totalCount +=1;
                }
                return matchCount / (double)totalCount;
        }
       
       
        public double ComplexDistance(NetflixMovie movie) {
                return 0.0;
        }
       
}

