package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * <p>
 * WritableComparable representing a pair consisting of a String and a floating
 * point number. The elements in the pair are referred to as the left and right
 * elements. The natural sort order is: first by the left element, and then by
 * the right element.
 * </p>
 * 
 * @author ferhanture
 * 
 */
public class PairOfStringFloat implements WritableComparable<PairOfStringFloat> {

	private String leftElement;
	private float rightElement;

	/**
	 * Creates a pair.
	 */
	public PairOfStringFloat() {
	}

	/**
	 * Creates a pair.
	 * 
	 * @param left
	 *            the left element
	 * @param right
	 *            the right element
	 */
	public PairOfStringFloat(String left, float right) {
		set(left, right);
	}

	/**
	 * Deserializes the pair.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		leftElement = in.readUTF();
		rightElement = in.readFloat();
	}

	/**
	 * Serializes this pair.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(leftElement);
		out.writeFloat(rightElement);
	}

	/**
	 * Returns the left element.
	 * 
	 * @return the left element
	 */
	public String getLeftElement() {
		return leftElement;
	}

	/**
	 * Returns the right element.
	 * 
	 * @return the right element
	 */
	public float getRightElement() {
		return rightElement;
	}

	/**
	 * Sets the right and left elements of this pair.
	 * 
	 * @param left
	 *            the left element
	 * @param right
	 *            the right element
	 */
	public void set(String left, float right) {
		leftElement = left;
		rightElement = right;
	}

	/**
	 * Checks two pairs for equality.
	 * 
	 * @param obj
	 *            object for comparison
	 * @return <code>true</code> if <code>obj</code> is equal to this
	 *         object, <code>false</code> otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		PairOfStringFloat pair = (PairOfStringFloat) obj;
		return leftElement.equals(pair.getLeftElement()) && rightElement == pair.getRightElement();
	}

	/**
	 * Defines a natural sort order for pairs. Pairs are sorted first by the
	 * left element, and then by the right element.
	 * 
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(PairOfStringFloat obj) {
		PairOfStringFloat pair = (PairOfStringFloat) obj;

		String pl = pair.getLeftElement();
		float pr = pair.getRightElement();

		if (leftElement.equals(pl)) {
			if (rightElement == pr)
				return 0;

			return rightElement < pr ? -1 : 1;
		}

		return leftElement.compareTo(pl);
	}

	/**
	 * Returns a hash code value for the pair.
	 * 
	 * @return hash code for the pair
	 */
	@Override
	public int hashCode() {
		return leftElement.hashCode() + (int) rightElement;
	}

	/**
	 * Generates human-readable String representation of this pair.
	 * 
	 * @return human-readable String representation of this pair
	 */
	@Override
	public String toString() {
		return "(" + leftElement + ", " + rightElement + ")";
	}

	/**
	 * Clones this object.
	 * 
	 * @return clone of this object
	 */
	@Override
	public PairOfStringFloat clone() {
		return new PairOfStringFloat(this.leftElement, this.rightElement);
	}

	/** Comparator optimized for <code>PairOfStringFloat</code>. */
	public static class Comparator extends WritableComparator {

		/**
		 * Creates a new Comparator optimized for <code>PairOfStrings</code>.
		 */
		public Comparator() {
			super(PairOfInts.class);
		}

		/**
		 * Optimization hook.
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			String thisLeftValue = readUTF(b1, s1);
			String thatLeftValue = readUTF(b2, s2);

			if (thisLeftValue.equals(thatLeftValue)) {
				int s1offset = readUnsignedShort(b1, s1);
				int s2offset = readUnsignedShort(b2, s2);

				float thisRightValue = readFloat(b1, s1 + 2 + s1offset);
				float thatRightValue = readFloat(b2, s2 + 2 + s2offset);

				return (thisRightValue < thatRightValue ? -1
						: (thisRightValue == thatRightValue ? 0 : 1));
			}

			return thisLeftValue.compareTo(thatLeftValue);
		}

		private String readUTF(byte[] bytes, int s) {

			try {

				int utflen = readUnsignedShort(bytes, s);

				byte[] bytearr = new byte[utflen];
				char[] chararr = new char[utflen];

				int c, char2, char3;
				int count = 0;
				int chararr_count = 0;

				System.arraycopy(bytes, s + 2, bytearr, 0, utflen);
				// in.readFully(bytearr, 0, utflen);

				while (count < utflen) {
					c = (int) bytearr[count] & 0xff;
					if (c > 127)
						break;
					count++;
					chararr[chararr_count++] = (char) c;
				}

				while (count < utflen) {
					c = (int) bytearr[count] & 0xff;
					switch (c >> 4) {
					case 0:
					case 1:
					case 2:
					case 3:
					case 4:
					case 5:
					case 6:
					case 7:
						/* 0xxxxxxx */
						count++;
						chararr[chararr_count++] = (char) c;
						break;
					case 12:
					case 13:
						/* 110x xxxx 10xx xxxx */
						count += 2;
						if (count > utflen)
							throw new UTFDataFormatException(
									"malformed input: partial character at end");
						char2 = (int) bytearr[count - 1];
						if ((char2 & 0xC0) != 0x80)
							throw new UTFDataFormatException("malformed input around byte " + count);
						chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
						break;
					case 14:
						/* 1110 xxxx 10xx xxxx 10xx xxxx */
						count += 3;
						if (count > utflen)
							throw new UTFDataFormatException(
									"malformed input: partial character at end");
						char2 = (int) bytearr[count - 2];
						char3 = (int) bytearr[count - 1];
						if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
							throw new UTFDataFormatException("malformed input around byte "
									+ (count - 1));
						chararr[chararr_count++] = (char) (((c & 0x0F) << 12)
								| ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
						break;
					default:
						/* 10xx xxxx, 1111 xxxx */
						throw new UTFDataFormatException("malformed input around byte " + count);
					}
				}
				// The number of chars produced may be less than utflen
				return new String(chararr, 0, chararr_count);

			} catch (Exception e) {
				e.printStackTrace();
			}

			return null;
		}
	}

	static { // register this comparator
		WritableComparator.define(PairOfStringFloat.class, new Comparator());
	}
}
