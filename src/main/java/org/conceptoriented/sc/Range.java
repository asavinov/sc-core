package org.conceptoriented.sc;

public class Range {

    public final long start;
    public final long end;

    @Override
    public String toString() {
      return String.format("[%s, %s)", start, end);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Range)){
            return false;
        }

        Range other_ = (Range)other;

        // this may cause NPE if nulls are valid values for x or y. The logic may be improved to handle nulls properly, if needed.
        return other_.start == this.start && other_.end == this.end;
    }

    @Override
    public int hashCode() {
    	return Long.hashCode(start) ^ Long.hashCode(end);
    }

    public Range(long start, long end) {
        super();
        this.start = start;
        this.end = end;
      }
}