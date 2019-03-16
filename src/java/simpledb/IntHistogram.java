package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    // fields
    private int max;
    private int min;
    private int width;
    private int lastWidth;
    private int numTuples;
    private int[] buckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.max = max;
        this.min = min;
        
        if (max - min + 1 < buckets) {
            this.buckets = new int[max - min + 1];
            width = 1;
            lastWidth = 1;
        } else {
            this.buckets = new int[buckets];
            width = (int) Math.floor((max - min + 1) / (double) buckets);
            lastWidth = (max - (min + (buckets - 1) * width)) + 1;
        }
        
        for (int i = 0; i < this.buckets.length; i++) {
            this.buckets[i] = 0;
        }
        
        numTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v < min || v > max) {
            throw new IllegalArgumentException("value to add out of range");
        }
        
        if ((v - min) / width < buckets.length - 1) {
            buckets[(v - min) / width]++;
        } else {
            buckets[buckets.length - 1]++;
        }
        
        numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int b;
        double b_f;
        double temp;
        
        switch (op) {
        case EQUALS:
            if (v < min || v > max) {
                return 0;
            }
            
            if ((v - min) / width < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
                    
            if (b < buckets.length - 1) {
                return (buckets[b] / (double) width) / numTuples;
            } else {
                return (buckets[b] / (double) lastWidth) / numTuples;
            }
            
        case GREATER_THAN:
            if (v < min) {
                return 1;
            } else if (v >= max) {
                return 0;
            }
            
            if ((v - min) / width < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
            
            b_f = buckets[b] / (double) numTuples;
            
            if (b < buckets.length - 1) {
                temp = (((min + (b + 1) * width) - v - 1) / (double) width) * b_f;
                
                for (int i = b + 1; i < buckets.length; i++) {
                    temp += buckets[i] / (double) numTuples;
                }
                
                return temp;
            } else {
                return ((max - v) / (double) lastWidth) * b_f;
            }
            
        case LESS_THAN:
            if (v <= min) {
                return 0;
            } else if (v > max) {
                return 1;
            }
            
            if ((v - min) / width < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
                    
            b_f = buckets[b] / (double) numTuples;
            
            if (b < buckets.length - 1) {
                temp = (((min + (b + 1) * width) - v - 1) / (double) width) * b_f;
                
                for (int i = b - 1; i >= 0; i--) {
                    temp += buckets[i] / (double) numTuples;
                }
                
                return temp;
            } else {
                temp = ((max - v) / (double) lastWidth) * b_f;
                
                for (int i = b - 1; i >= 0; i--) {
                    temp += buckets[i] / (double) numTuples;
                }
                return temp;
            }
            
        case LESS_THAN_OR_EQ:
            if (v < min) {
                return 0;
            } else if (v >= max) {
                return 1;
            }
            
            if ((v - min) / width < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
                    
            b_f = buckets[b] / (double) numTuples;
            
            if (b < buckets.length - 1) {
                temp = (((min + (b + 1) * width) - v - 1) / (double) width) * b_f;
                
                for (int i = b + 1; i < buckets.length; i++) {
                    temp += buckets[i] / (double) numTuples;
                }
                
                return 1 - temp;
            } else {
                return 1 - ((max - v) / (double) lastWidth) * b_f;
            }
            
        case GREATER_THAN_OR_EQ:
            if (v <= min) {
                return 1; 
            } else if (v > max) {
                return 0;
            }
            
            if ((v - min) / width < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
                    
            b_f = buckets[b] / (double) numTuples;
            
            if (b < buckets.length - 1) {
                temp = (((min + (b + 1) * width) - v) / (double) width) * b_f;
                
                for (int i = b + 1; i < buckets.length; i++) {
                    temp += buckets[i] / ((double) numTuples);
                }
                
                return temp;
            } else {
                return ((max - v + 1) / (double) lastWidth) * b_f;
            }
            
        case LIKE:
            if (v < min || v > max) {
                return 0;
            }
            
            if (((v - min) / width) < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
                    
            if (b < buckets.length - 1) {
                return (buckets[b] / (double) width) / numTuples;
            } else {
                return (buckets[b] / (double) lastWidth) / numTuples;
            }
            
        case NOT_EQUALS:
            if (v < min || v > max) {
                return 1;
            }
            
            if ((v - min) / width < buckets.length - 1) {
                b = (v - min) / width;
            } else {
                b = buckets.length - 1;
            }
            
            if (b < buckets.length - 1) {
                return 1 - ((buckets[b] / (double) width) / numTuples);
            } else {
                return 1 - ((buckets[b] / (double) lastWidth) / numTuples);
            }
            
        default:
            throw new IllegalArgumentException();
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String str = "IntHistogram: \r\n";
        
        for (int i = 0; i < buckets.length; i++) {
            if (i < buckets.length - 1) {
                str += "    " + ((max - min) * i + min) + " - " + 
                        ((max - min) * (i + 1) + min) + " : " + buckets[i] + "\r\n";
            } else {
                str += "    " + ((max - min) * i + min) + " - " + 
                        max + " : " + buckets[i] + "\r\n";
            }
        }
        
        return str;
    }
}
