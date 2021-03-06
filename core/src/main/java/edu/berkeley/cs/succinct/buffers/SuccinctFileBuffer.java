package edu.berkeley.cs.succinct.buffers;

import edu.berkeley.cs.succinct.StorageMode;
import edu.berkeley.cs.succinct.SuccinctFile;
import edu.berkeley.cs.succinct.regex.RegExMatch;
import edu.berkeley.cs.succinct.regex.SuccinctRegEx;
import edu.berkeley.cs.succinct.regex.parser.RegExParsingException;
import edu.berkeley.cs.succinct.util.Range;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class SuccinctFileBuffer extends SuccinctBuffer implements SuccinctFile {

  private static final long serialVersionUID = 5879363803993345049L;
  protected transient long fileOffset;

  /**
   * Constructor to create SuccinctBuffer from byte array, context length and file offset.
   *
   * @param input      Input byte array.
   * @param contextLen Context length.
   * @param fileOffset Beginning offset for this file chunk (if file is partitioned).
   */
  public SuccinctFileBuffer(byte[] input, int contextLen, long fileOffset) {
    super(input, contextLen);
    this.fileOffset = fileOffset;
  }

  /**
   * Constructor to create SuccinctBuffer from byte array and file offset.
   *
   * @param input      Input byte array.
   * @param fileOffset Beginning offset for this file chunk (if file is partitioned).
   */
  public SuccinctFileBuffer(byte[] input, long fileOffset) {
    this(input, 3, fileOffset);
  }

  /**
   * Constructor to create SuccinctBuffer from byte array.
   *
   * @param input Input byte array.
   */
  public SuccinctFileBuffer(byte[] input) {
    this(input, 0);
  }

  /**
   * Constructor to load the data from persisted Succinct data-structures.
   *
   * @param path        Path to load data from.
   * @param storageMode Mode in which data is stored (In-memory or Memory-mapped)
   */
  public SuccinctFileBuffer(String path, StorageMode storageMode) {
    super(path, storageMode);
  }

  /**
   * Constructor to load the data from a ByteBuffer.
   *
   * @param buf Input buffer to load the data from
   */
  public SuccinctFileBuffer(ByteBuffer buf) {
    super(buf);
  }

  /**
   * Default constructor.
   */
  public SuccinctFileBuffer() {
    super();
  }

  /**
   * Get beginning offset for the file chunk.
   *
   * @return The beginning offset for the file chunk.
   */
  public long getFileOffset() {
    return fileOffset;
  }

  /**
   * Get offset range for the file chunk.
   *
   * @return The offset range for the file chunk.
   */
  public Range getFileRange() {
    return new Range(fileOffset, fileOffset + getOriginalSize() - 2);
  }

  /**
   * Get the alphabet for the succinct file.
   *
   * @return The alphabet for the succinct file.
   */
  @Override public byte[] getAlphabet() {
    byte[] alphabetBuf = new byte[getAlphaSize()];
    alphabet.buffer().get(alphabetBuf);
    alphabet.rewind();
    return alphabetBuf;
  }

  /**
   * Get the character at index in file.
   *
   * @param i Index into file.
   * @return The character at the specified index.
   */
  public char charAt(long i) {
    return (char) alphabet.get((int) lookupC(lookupISA(i - fileOffset)));
  }

  /**
   * Extract data of specified length from Succinct data structures at specified index.
   *
   * @param offset Index into original input to start extracting at.
   * @param len    Length of data to be extracted.
   * @return Extracted data.
   */
  @Override public byte[] extract(long offset, int len) {

    byte[] buf = new byte[len];
    long s;

    long chunkOffset = offset - fileOffset;
    s = lookupISA(chunkOffset);
    for (int k = 0; k < len && k < getOriginalSize(); k++) {
      buf[k] = alphabet.get((int) lookupC(s));
      s = lookupNPA(s);
    }

    return buf;
  }

  /**
   * Extract data from Succinct data structures at specified index until specified delimiter.
   *
   * @param offset Index into original input to start extracting at.
   * @param delim  Delimiter at which to stop extracting.
   * @return Extracted data.
   */
  @Override public byte[] extractUntil(long offset, byte delim) {

    String strBuf = "";
    long s;

    long chunkOffset = offset - fileOffset;
    s = lookupISA(chunkOffset);
    char nextChar;
    do {
      nextChar = (char) alphabet.get((int) lookupC(s));
      if (nextChar == delim || nextChar == 1)
        break;
      strBuf += nextChar;
      s = lookupNPA(s);
    } while (true);

    return strBuf.getBytes();
  }

  /**
   * Perform backward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range bwdSearch(byte[] buf) {
    Range range = new Range(0L, -1L);
    int m = buf.length;
    long c1, c2;

    if (alphabetMap.containsKey(buf[m - 1])) {
      range.first = alphabetMap.get(buf[m - 1]).first;
      range.second =
        alphabetMap.get((alphabet.get(alphabetMap.get(buf[m - 1]).second + 1))).first - 1;
    } else {
      return range;
    }

    for (int i = m - 2; i >= 0; i--) {
      if (alphabetMap.containsKey(buf[i])) {
        c1 = alphabetMap.get(buf[i]).first;
        c2 = alphabetMap.get((alphabet.get(alphabetMap.get(buf[i]).second + 1))).first - 1;
      } else {
        return range;
      }
      range.first = binSearchNPA(range.first, c1, c2, false);
      range.second = binSearchNPA(range.second, c1, c2, true);
    }

    return range;
  }

  /**
   * Continue backward search on query to obtain SA range.
   *
   * @param buf   Input query.
   * @param range Range to start from.
   * @return Range into SA.
   */
  @Override public Range continueBwdSearch(byte[] buf, Range range) {
    Range newRange = new Range(range.first, range.second);
    int m = buf.length;
    long c1, c2;

    for (int i = m - 1; i >= 0; i--) {
      if (alphabetMap.containsKey(buf[i])) {
        c1 = alphabetMap.get(buf[i]).first;
        c2 = alphabetMap.get((alphabet.get(alphabetMap.get(buf[i]).second + 1))).first - 1;
      } else {
        return newRange;
      }
      newRange.first = binSearchNPA(newRange.first, c1, c2, false);
      newRange.second = binSearchNPA(newRange.second, c1, c2, true);
    }
    return newRange;
  }

  /**
   * Compare entire buffer with input starting at specified index.
   *
   * @param buf The buffer to compare with.
   * @param i   The index into input.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(byte[] buf, int i) {
    int j = 0;

    do {
      byte c = alphabet.get((int) lookupC(i));
      if (buf[j] < c) {
        return -1;
      } else if (buf[j] > c) {
        return 1;
      }
      i = (int) lookupNPA(i);
      j++;
    } while (j < buf.length);

    return 0;
  }

  /**
   * Compare entire buffer with input starting at specified index and offset
   * into buffer.
   *
   * @param buf    The buffer to compare with.
   * @param i      The index into input.
   * @param offset Offset into buffer.
   * @return -1 if buf is smaller, 0 if equal and 1 if buf is greater.
   */
  @Override public int compare(byte[] buf, int i, int offset) {
    int j = 0;

    while (offset != 0) {
      i = (int) lookupNPA(i);
      offset--;
    }

    do {
      byte c = alphabet.get((int) lookupC(i));
      if (buf[j] < c) {
        return -1;
      } else if (buf[j] > c) {
        return 1;
      }
      i = (int) lookupNPA(i);
      j++;
    } while (j < buf.length);

    return 0;
  }

  /**
   * Perform forward search to obtain SA range for a query.
   *
   * @param buf Input query.
   * @return Range into SA.
   */
  @Override public Range fwdSearch(byte[] buf) {
    int st = getOriginalSize() - 1;
    int sp = 0;
    int s;
    while (sp < st) {
      s = (sp + st) / 2;
      if (compare(buf, s) > 0) {
        sp = s + 1;
      } else {
        st = s;
      }
    }

    int et = getOriginalSize() - 1;
    int ep = sp - 1;
    int e;
    while (ep < et) {
      e = (int) Math.ceil((double) (ep + et) / 2);
      if (compare(buf, e) == 0) {
        ep = e;
      } else {
        et = e - 1;
      }
    }

    return new Range(sp, ep);
  }

  /**
   * Continue forward search on query to obtain SA range.
   *
   * @param buf    Input query.
   * @param range  Range to start from.
   * @param offset Offset into input query.
   * @return Range into SA.
   */
  @Override public Range continueFwdSearch(byte[] buf, Range range, int offset) {

    if (buf.length == 0) {
      return range;
    }

    int st = (int) range.second;
    int sp = (int) range.first;
    int s;
    while (sp < st) {
      s = (sp + st) / 2;
      if (compare(buf, s, offset) > 0) {
        sp = sp + 1;
      } else {
        st = s;
      }
    }

    int et = (int) range.second;
    int ep = sp - 1;
    int e;
    while (ep < et) {
      e = (int) Math.ceil((double) (ep + et) / 2);
      if (compare(buf, e, offset) == 0) {
        ep = e;
      } else {
        et = e - 1;
      }
    }

    return new Range(sp, ep);
  }

  /**
   * Get count of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return Count of occurrences.
   */
  @Override public long count(byte[] query) {
    Range range = bwdSearch(query);
    return range.second - range.first + 1;
  }

  /**
   * Translate range into SA to offsets in file.
   *
   * @param range Range into SA.
   * @return Offsets corresponding to offsets.
   */
  @Override public Long[] rangeToOffsets(Range range) {
    if (range.empty()) {
      return new Long[0];
    }

    Long[] offsets = new Long[(int) range.size()];
    for (long i = 0; i < range.size(); i++) {
      offsets[((int) i)] = lookupSA(range.begin() + i) + fileOffset;
    }

    return offsets;
  }

  /**
   * Get all locations of pattern occurrences in original input.
   *
   * @param query Input query.
   * @return All locations of pattern occurrences in original input.
   */
  @Override public Long[] search(byte[] query) {
    return rangeToOffsets(bwdSearch(query));
  }


  /**
   * Check if the two offsets belong to the same record. This is always true for the
   * SuccinctFileBuffer.
   *
   * @param firstOffset The first offset.
   * @param secondOffset The second offset.
   * @return True if the two offsets belong to the same record, false otherwise.
   */
  @Override public boolean sameRecord(long firstOffset, long secondOffset) {
    return true;
  }

  /**
   * Performs regular expression search for an input expression using Succinct data-structures.
   *
   * @param query Regular expression pattern to be matched. (UTF-8 encoded)
   * @return All locations and lengths of matching patterns in original input.
   * @throws RegExParsingException
   */
  @Override public Map<Long, Integer> regexSearch(String query) throws RegExParsingException {
    SuccinctRegEx succinctRegEx = new SuccinctRegEx(this, query);

    Set<RegExMatch> chunkResults = succinctRegEx.compute();
    Map<Long, Integer> results = new TreeMap<Long, Integer>();
    for (RegExMatch result : chunkResults) {
      results.put(result.getOffset(), result.getLength());
    }

    return results;
  }

  /**
   * Write Succinct data structures to a DataOutputStream.
   *
   * @param os Output stream to write data to.
   * @throws IOException
   */
  @Override public void writeToStream(DataOutputStream os) throws IOException {
    super.writeToStream(os);
    os.writeLong(fileOffset);
  }

  /**
   * Reads Succinct data structures from a DataInputStream.
   *
   * @param is Stream to read data structures from.
   * @throws IOException
   */
  @Override public void readFromStream(DataInputStream is) throws IOException {
    super.readFromStream(is);
    fileOffset = is.readLong();
  }

  /**
   * Reads Succinct data structures from a ByteBuffer.
   *
   * @param buf ByteBuffer to read Succinct data structures from.
   */
  @Override public void mapFromBuffer(ByteBuffer buf) {
    super.mapFromBuffer(buf);
    fileOffset = buf.getLong();
  }

  /**
   * Serialize SuccinctIndexedBuffer to OutputStream.
   *
   * @param oos ObjectOutputStream to write to.
   * @throws IOException
   */
  private void writeObject(ObjectOutputStream oos) throws IOException {
    oos.writeLong(fileOffset);
  }

  /**
   * Deserialize SuccinctIndexedBuffer from InputStream.
   *
   * @param ois ObjectInputStream to read from.
   * @throws IOException
   */
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    fileOffset = ois.readLong();
  }
}
