package data_algorithms_book.chap05.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class PairOfWords implements WritableComparable<PairOfWords>{
	
	private String leftElement;
	private String rightElement;
	
	public PairOfWords(){}
	
	public PairOfWords(String left, String right) {
		set(left, right);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		leftElement = Text.readString(in);
		rightElement = Text.readString(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, leftElement);
		Text.writeString(out, rightElement);
	}
	
	public void setLeftElement(String leftElement) {
        this.leftElement = leftElement;
    }

    public void setWord(String leftElement) {
        setLeftElement(leftElement);
    }
    
    public String getWord() {
        return leftElement;
    }
    
    public String getLeftElement() {
        return leftElement;
    }

    public void setRightElement(String rightElement) {
        this.rightElement = rightElement;
    }
    
    public void setNeighbor(String rightElement) {
        setRightElement(rightElement);
    }
    
    public String getRightElement() {
        return rightElement;
    }

    public String getNeighbor() {
        return rightElement;
    }
    
    public String getKey() {
        return leftElement;
    }
    
    public String getValue() {
        return rightElement;
    }
    
    public void set(String left, String right) {
        leftElement = left;
        rightElement = right;
    }
    
    @Override
    public boolean equals(Object obj) {
    	if (obj == null) {
    		return false;
    	}
    	if (!( obj instanceof PairOfWords)) {
    		return false;
    	}
    	PairOfWords pair = (PairOfWords) obj;
    	return leftElement.equals(pair.getLeftElement())
    				&& rightElement.equals(pair.getRightElement());
    }
    
    @Override
    public int compareTo(PairOfWords pair) {
    	String pl = pair.getLeftElement();
    	String pr = pair.getRightElement();
    	
    	if (leftElement.equals(pl)) {
    		return rightElement.compareTo(pr);
    	} 
    	return leftElement.compareTo(pl);
    	
    }
    
    @Override
    public int hashCode() {
    	return leftElement.hashCode() + rightElement.hashCode();
    }
    
    @Override
    public String toString(){
    	return "(" + leftElement + ", " + rightElement + ")";
    }
    
    @Override
    public PairOfWords clone() {
    	return new PairOfWords(this.leftElement, this.rightElement);
    }
    
    public static class Comparator extends WritableComparator {
    	public Comparator() {
    		super(PairOfWords.class);
    	}
    	
    	@Override
    	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    		try {
    			int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
    			int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
    			int firstStrL1 = readVInt(b1, s1);
    			int firstStrL2 = readVInt(b2, s2);
    			int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1, b2, s2 + firstVIntL2, firstStrL2);
    			if (cmp != 0 ) {
    				return cmp;
    			}
    			
    			int secondVIntL1 = WritableUtils.decodeVIntSize(b1[s1 + firstVIntL1 + firstStrL1]);
                int secondVIntL2 = WritableUtils.decodeVIntSize(b2[s2 + firstVIntL2 + firstStrL2]);
                int secondStrL1 = readVInt(b1, s1 + firstVIntL1 + firstStrL1);
                int secondStrL2 = readVInt(b2, s2 + firstVIntL2 + firstStrL2);
                return compareBytes(b1, s1 + firstVIntL1 + firstStrL1 + secondVIntL1, secondStrL1, b2,
                        s2 + firstVIntL2 + firstStrL2 + secondVIntL2, secondStrL2); 
    		} catch (IOException e) {
    			throw new IllegalArgumentException(e);
    		}
    	}
    }
    
    static {
    	WritableComparator.define(PairOfWords.class, new Comparator());
    }
}
