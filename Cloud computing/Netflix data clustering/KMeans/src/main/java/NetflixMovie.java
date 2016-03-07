import java.util.*;
import java.io.Serializable;

public class NetflixMovie implements Serializable{
   
        public int movie_id;
        public Map<Integer,Double> features; 
        
				public NetflixMovie(int id, Map<Integer,Double> temp){
					this.movie_id = id;
					this.features = temp;
				}
				
        public NetflixMovie(int id, String data) {
          this.movie_id = id;
          String[] toAdd = data.split("\\|");
					this.features = new TreeMap<Integer,Double>();
          for(String s: toAdd){
						String[] data1 = s.split(":");
						this.features.put(Integer.parseInt(data1[0]),Double.parseDouble(data1[1]));
					}
        }
			
        public int MatchCount(NetflixMovie movie, int thresh) {
                Iterator<Integer> it1 = this.features.keySet().iterator();
                Iterator<Integer> it2 = movie.features.keySet().iterator();
                int matchCount = 0;
                Integer one = null;
                Integer two = null;
                while((it1.hasNext())&&(it2.hasNext())) {
                        if((one==null)&&(two==null)) {
                                one = (Integer)it1.next();
                                two = (Integer)it2.next();
                                continue;
                        }
                        if(one.equals(two)) {
                                matchCount += 1;
                                one = (Integer)it1.next();
                                two = (Integer)it2.next();
                        } else if(one.compareTo(two)<0)
                                one = (Integer)it1.next();
                        else
                                two = (Integer)it2.next();
                        if(matchCount>thresh)
                                break;
                }
                return matchCount;
        }

        
        public int MatchCount(NetflixMovie movie) {
                Iterator<Integer> it1 = this.features.keySet().iterator();
                Iterator<Integer> it2 = movie.features.keySet().iterator();
                int matchCount = 0;
                Integer one = null;
                Integer two = null;
                while((it1.hasNext())&&(it2.hasNext())) {
                        if((one==null)&&(two==null)) {
                                one = (Integer)it1.next();
                                two = (Integer)it2.next();
                        }
                        if(one.equals(two)) {
                                matchCount += 1;
                                one = (Integer)it1.next();
                                two = (Integer)it2.next();
                        } else if(one.compareTo(two)<0)
                                one = (Integer)it1.next();
                        else
                                two = (Integer)it2.next();
                }
                return matchCount;
        }

        public double SimpleDistance(NetflixMovie movie) {
                Iterator<Integer> it1 = this.features.keySet().iterator();
                Iterator<Integer> it2 = movie.features.keySet().iterator();
                int matchCount = 0;
                int totalCount = 1;
                Integer one = null;
                Integer two = null;
                while((it1.hasNext())&&(it2.hasNext())) {
                        if((one==null)&&(two==null)) {
                                one = new Integer((Integer)it1.next());
                                two = new Integer((Integer)it2.next());
                        }
                        if(one.equals(two)) {
                                matchCount += 1;
                                one = new Integer((Integer)it1.next());
                                two = new Integer((Integer)it2.next());
                        } else if(one.compareTo(two)<0)
                                one = new Integer((Integer)it1.next());
                        else
                                two = new Integer((Integer)it2.next());
                        totalCount +=1;
                }
                return matchCount / (double)totalCount;
        }
        

        public double ComplexDistance(NetflixMovie movie) {
                Iterator<Map.Entry<Integer, Double>> it1 = this.features.entrySet().iterator();
                Iterator<Map.Entry<Integer, Double>> it2 = movie.features.entrySet().iterator();
                double dotProduct = 0.0;
                double magOne = 0.0;
                double magTwo = 0.0;
                Map.Entry one = null;
                Map.Entry two = null;
                while((it1.hasNext())&&(it2.hasNext())) {
                        if((one==null)&&(two==null)) {
                                one = (Map.Entry)it1.next();
                                two = (Map.Entry)it2.next();
                        }
                        if((new Integer((Integer)one.getKey())).equals(new Integer((Integer)two.getKey()))) {
                                dotProduct += (Double)one.getValue()*(Double)two.getValue();
																magOne += (Double)one.getValue()*(Double)one.getValue();
																magTwo += (Double)two.getValue()*(Double)two.getValue();
                                one = (Map.Entry)it1.next();
                                two = (Map.Entry)it2.next();
                        } else if((new Integer((Integer)one.getKey())).compareTo(new Integer((Integer)two.getKey()))<0) {
                                magOne += (Double)one.getValue()*(Double)one.getValue();
                                one = (Map.Entry)it1.next();
                        }else {
                                magTwo += (Double)two.getValue()*(Double)two.getValue();
                                two = (Map.Entry)it2.next();
                        }
                }
                while(it1.hasNext()) {
                        Map.Entry t = (Map.Entry)it1.next();
                        magOne += (Double)t.getValue()*(Double)t.getValue();
                }
                while(it2.hasNext()){
                        Map.Entry t = (Map.Entry)it2.next();
                        magTwo += (Double)t.getValue()*(Double)t.getValue();                    
                }
                return (dotProduct) / Math.sqrt(magOne*magTwo);
        }
        
}