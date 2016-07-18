/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package ids;

import com.google.gson.Gson;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class IDs extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord, Writable {
  private Gson gson = new Gson();
  private static final long serialVersionUID = 4758961859436357763L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IDs\",\"namespace\":\"ids\",\"fields\":[{\"name\":\"Global_Id\",\"type\":\"string\",\"avro.java.string\":\"String\"},{\"name\":\"Imei\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Mac\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Imsi\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Phone_Number\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Idfa\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Openudid\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Uid\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}},{\"name\":\"Did\",\"type\":{\"type\":\"map\",\"values\":\"int\",\"avro.java.string\":\"String\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public String Global_Id;
  @Deprecated public java.util.Map<String,Integer> Imei;
  @Deprecated public java.util.Map<String,Integer> Mac;
  @Deprecated public java.util.Map<String,Integer> Imsi;
  @Deprecated public java.util.Map<String,Integer> Phone_Number;
  @Deprecated public java.util.Map<String,Integer> Idfa;
  @Deprecated public java.util.Map<String,Integer> Openudid;
  @Deprecated public java.util.Map<String,Integer> Uid;
  @Deprecated public java.util.Map<String,Integer> Did;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public IDs() {}

  /**
   * All-args constructor.
   * @param Global_Id The new value for Global_Id
   * @param Imei The new value for Imei
   * @param Mac The new value for Mac
   * @param Imsi The new value for Imsi
   * @param Phone_Number The new value for Phone_Number
   * @param Idfa The new value for Idfa
   * @param Openudid The new value for Openudid
   * @param Uid The new value for Uid
   * @param Did The new value for Did
   */
  public IDs(String Global_Id, java.util.Map<String,Integer> Imei, java.util.Map<String,Integer> Mac, java.util.Map<String,Integer> Imsi, java.util.Map<String,Integer> Phone_Number, java.util.Map<String,Integer> Idfa, java.util.Map<String,Integer> Openudid, java.util.Map<String,Integer> Uid, java.util.Map<String,Integer> Did) {
    this.Global_Id = Global_Id;
    this.Imei = Imei;
    this.Mac = Mac;
    this.Imsi = Imsi;
    this.Phone_Number = Phone_Number;
    this.Idfa = Idfa;
    this.Openudid = Openudid;
    this.Uid = Uid;
    this.Did = Did;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public Object get(int field$) {
    switch (field$) {
    case 0: return Global_Id;
    case 1: return Imei;
    case 2: return Mac;
    case 3: return Imsi;
    case 4: return Phone_Number;
    case 5: return Idfa;
    case 6: return Openudid;
    case 7: return Uid;
    case 8: return Did;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, Object value$) {
    switch (field$) {
    case 0: Global_Id = (String)value$; break;
    case 1: Imei = (java.util.Map<String,Integer>)value$; break;
    case 2: Mac = (java.util.Map<String,Integer>)value$; break;
    case 3: Imsi = (java.util.Map<String,Integer>)value$; break;
    case 4: Phone_Number = (java.util.Map<String,Integer>)value$; break;
    case 5: Idfa = (java.util.Map<String,Integer>)value$; break;
    case 6: Openudid = (java.util.Map<String,Integer>)value$; break;
    case 7: Uid = (java.util.Map<String,Integer>)value$; break;
    case 8: Did = (java.util.Map<String,Integer>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'Global_Id' field.
   * @return The value of the 'Global_Id' field.
   */
  public String getGlobalId() {
    return Global_Id;
  }

  /**
   * Sets the value of the 'Global_Id' field.
   * @param value the value to set.
   */
  public void setGlobalId(String value) {
    this.Global_Id = value;
  }

  /**
   * Gets the value of the 'Imei' field.
   * @return The value of the 'Imei' field.
   */
  public java.util.Map<String,Integer> getImei() {
    return Imei;
  }

  /**
   * Sets the value of the 'Imei' field.
   * @param value the value to set.
   */
  public void setImei(java.util.Map<String,Integer> value) {
    this.Imei = value;
  }

  /**
   * Gets the value of the 'Mac' field.
   * @return The value of the 'Mac' field.
   */
  public java.util.Map<String,Integer> getMac() {
    return Mac;
  }

  /**
   * Sets the value of the 'Mac' field.
   * @param value the value to set.
   */
  public void setMac(java.util.Map<String,Integer> value) {
    this.Mac = value;
  }

  /**
   * Gets the value of the 'Imsi' field.
   * @return The value of the 'Imsi' field.
   */
  public java.util.Map<String,Integer> getImsi() {
    return Imsi;
  }

  /**
   * Sets the value of the 'Imsi' field.
   * @param value the value to set.
   */
  public void setImsi(java.util.Map<String,Integer> value) {
    this.Imsi = value;
  }

  /**
   * Gets the value of the 'Phone_Number' field.
   * @return The value of the 'Phone_Number' field.
   */
  public java.util.Map<String,Integer> getPhoneNumber() {
    return Phone_Number;
  }

  /**
   * Sets the value of the 'Phone_Number' field.
   * @param value the value to set.
   */
  public void setPhoneNumber(java.util.Map<String,Integer> value) {
    this.Phone_Number = value;
  }

  /**
   * Gets the value of the 'Idfa' field.
   * @return The value of the 'Idfa' field.
   */
  public java.util.Map<String,Integer> getIdfa() {
    return Idfa;
  }

  /**
   * Sets the value of the 'Idfa' field.
   * @param value the value to set.
   */
  public void setIdfa(java.util.Map<String,Integer> value) {
    this.Idfa = value;
  }

  /**
   * Gets the value of the 'Openudid' field.
   * @return The value of the 'Openudid' field.
   */
  public java.util.Map<String,Integer> getOpenudid() {
    return Openudid;
  }

  /**
   * Sets the value of the 'Openudid' field.
   * @param value the value to set.
   */
  public void setOpenudid(java.util.Map<String,Integer> value) {
    this.Openudid = value;
  }

  /**
   * Gets the value of the 'Uid' field.
   * @return The value of the 'Uid' field.
   */
  public java.util.Map<String,Integer> getUid() {
    return Uid;
  }

  /**
   * Sets the value of the 'Uid' field.
   * @param value the value to set.
   */
  public void setUid(java.util.Map<String,Integer> value) {
    this.Uid = value;
  }

  /**
   * Gets the value of the 'Did' field.
   * @return The value of the 'Did' field.
   */
  public java.util.Map<String,Integer> getDid() {
    return Did;
  }

  /**
   * Sets the value of the 'Did' field.
   * @param value the value to set.
   */
  public void setDid(java.util.Map<String,Integer> value) {
    this.Did = value;
  }

  /**
   * Creates a new IDs RecordBuilder.
   * @return A new IDs RecordBuilder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new IDs RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new IDs RecordBuilder
   */
  public static Builder newBuilder(Builder other) {
    return new Builder(other);
  }

  /**
   * Creates a new IDs RecordBuilder by copying an existing IDs instance.
   * @param other The existing instance to copy.
   * @return A new IDs RecordBuilder
   */
  public static Builder newBuilder(IDs other) {
    return new Builder(other);
  }

  public void write(DataOutput dataOutput) throws IOException {
    byte[] bytes = this.toString().getBytes();
    dataOutput.writeInt(bytes.length);
    dataOutput.write(bytes, 0, bytes.length);
  }

  public void readFields(DataInput dataInput) throws IOException {
    int length = dataInput.readInt();
    byte[] bytes = new byte[length];
    dataInput.readFully(bytes, 0, length);
    IDs id = (IDs)this.gson.fromJson(new String(bytes), IDs.class);
    this.setGlobalId(id.getGlobalId());
    this.setUid(id.getUid());
    this.setPhoneNumber(id.getPhoneNumber());
    this.setOpenudid(id.getOpenudid());
    this.setMac(id.getMac());
    this.setIdfa(id.getIdfa());
    this.setImei(id.getImei());
    this.setImsi(id.getImsi());
    this.setDid(id.getDid());
  }

  /**
   * RecordBuilder for IDs instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<IDs>
    implements org.apache.avro.data.RecordBuilder<IDs> {

    private String Global_Id;
    private java.util.Map<String,Integer> Imei;
    private java.util.Map<String,Integer> Mac;
    private java.util.Map<String,Integer> Imsi;
    private java.util.Map<String,Integer> Phone_Number;
    private java.util.Map<String,Integer> Idfa;
    private java.util.Map<String,Integer> Openudid;
    private java.util.Map<String,Integer> Uid;
    private java.util.Map<String,Integer> Did;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.Global_Id)) {
        this.Global_Id = data().deepCopy(fields()[0].schema(), other.Global_Id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Imei)) {
        this.Imei = data().deepCopy(fields()[1].schema(), other.Imei);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Mac)) {
        this.Mac = data().deepCopy(fields()[2].schema(), other.Mac);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.Imsi)) {
        this.Imsi = data().deepCopy(fields()[3].schema(), other.Imsi);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.Phone_Number)) {
        this.Phone_Number = data().deepCopy(fields()[4].schema(), other.Phone_Number);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.Idfa)) {
        this.Idfa = data().deepCopy(fields()[5].schema(), other.Idfa);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.Openudid)) {
        this.Openudid = data().deepCopy(fields()[6].schema(), other.Openudid);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.Uid)) {
        this.Uid = data().deepCopy(fields()[7].schema(), other.Uid);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.Did)) {
        this.Did = data().deepCopy(fields()[8].schema(), other.Did);
        fieldSetFlags()[8] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing IDs instance
     * @param other The existing instance to copy.
     */
    private Builder(IDs other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.Global_Id)) {
        this.Global_Id = data().deepCopy(fields()[0].schema(), other.Global_Id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.Imei)) {
        this.Imei = data().deepCopy(fields()[1].schema(), other.Imei);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.Mac)) {
        this.Mac = data().deepCopy(fields()[2].schema(), other.Mac);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.Imsi)) {
        this.Imsi = data().deepCopy(fields()[3].schema(), other.Imsi);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.Phone_Number)) {
        this.Phone_Number = data().deepCopy(fields()[4].schema(), other.Phone_Number);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.Idfa)) {
        this.Idfa = data().deepCopy(fields()[5].schema(), other.Idfa);
        fieldSetFlags()[5] = true;
      }
      if (isValidValue(fields()[6], other.Openudid)) {
        this.Openudid = data().deepCopy(fields()[6].schema(), other.Openudid);
        fieldSetFlags()[6] = true;
      }
      if (isValidValue(fields()[7], other.Uid)) {
        this.Uid = data().deepCopy(fields()[7].schema(), other.Uid);
        fieldSetFlags()[7] = true;
      }
      if (isValidValue(fields()[8], other.Did)) {
        this.Did = data().deepCopy(fields()[8].schema(), other.Did);
        fieldSetFlags()[8] = true;
      }
    }

    /**
      * Gets the value of the 'Global_Id' field.
      * @return The value.
      */
    public String getGlobalId() {
      return Global_Id;
    }

    /**
      * Sets the value of the 'Global_Id' field.
      * @param value The value of 'Global_Id'.
      * @return This builder.
      */
    public Builder setGlobalId(String value) {
      validate(fields()[0], value);
      this.Global_Id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'Global_Id' field has been set.
      * @return True if the 'Global_Id' field has been set, false otherwise.
      */
    public boolean hasGlobalId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'Global_Id' field.
      * @return This builder.
      */
    public Builder clearGlobalId() {
      Global_Id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'Imei' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getImei() {
      return Imei;
    }

    /**
      * Sets the value of the 'Imei' field.
      * @param value The value of 'Imei'.
      * @return This builder.
      */
    public Builder setImei(java.util.Map<String,Integer> value) {
      validate(fields()[1], value);
      this.Imei = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'Imei' field has been set.
      * @return True if the 'Imei' field has been set, false otherwise.
      */
    public boolean hasImei() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'Imei' field.
      * @return This builder.
      */
    public Builder clearImei() {
      Imei = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'Mac' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getMac() {
      return Mac;
    }

    /**
      * Sets the value of the 'Mac' field.
      * @param value The value of 'Mac'.
      * @return This builder.
      */
    public Builder setMac(java.util.Map<String,Integer> value) {
      validate(fields()[2], value);
      this.Mac = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'Mac' field has been set.
      * @return True if the 'Mac' field has been set, false otherwise.
      */
    public boolean hasMac() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'Mac' field.
      * @return This builder.
      */
    public Builder clearMac() {
      Mac = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'Imsi' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getImsi() {
      return Imsi;
    }

    /**
      * Sets the value of the 'Imsi' field.
      * @param value The value of 'Imsi'.
      * @return This builder.
      */
    public Builder setImsi(java.util.Map<String,Integer> value) {
      validate(fields()[3], value);
      this.Imsi = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'Imsi' field has been set.
      * @return True if the 'Imsi' field has been set, false otherwise.
      */
    public boolean hasImsi() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'Imsi' field.
      * @return This builder.
      */
    public Builder clearImsi() {
      Imsi = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'Phone_Number' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getPhoneNumber() {
      return Phone_Number;
    }

    /**
      * Sets the value of the 'Phone_Number' field.
      * @param value The value of 'Phone_Number'.
      * @return This builder.
      */
    public Builder setPhoneNumber(java.util.Map<String,Integer> value) {
      validate(fields()[4], value);
      this.Phone_Number = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'Phone_Number' field has been set.
      * @return True if the 'Phone_Number' field has been set, false otherwise.
      */
    public boolean hasPhoneNumber() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'Phone_Number' field.
      * @return This builder.
      */
    public Builder clearPhoneNumber() {
      Phone_Number = null;
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'Idfa' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getIdfa() {
      return Idfa;
    }

    /**
      * Sets the value of the 'Idfa' field.
      * @param value The value of 'Idfa'.
      * @return This builder.
      */
    public Builder setIdfa(java.util.Map<String,Integer> value) {
      validate(fields()[5], value);
      this.Idfa = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'Idfa' field has been set.
      * @return True if the 'Idfa' field has been set, false otherwise.
      */
    public boolean hasIdfa() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'Idfa' field.
      * @return This builder.
      */
    public Builder clearIdfa() {
      Idfa = null;
      fieldSetFlags()[5] = false;
      return this;
    }

    /**
      * Gets the value of the 'Openudid' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getOpenudid() {
      return Openudid;
    }

    /**
      * Sets the value of the 'Openudid' field.
      * @param value The value of 'Openudid'.
      * @return This builder.
      */
    public Builder setOpenudid(java.util.Map<String,Integer> value) {
      validate(fields()[6], value);
      this.Openudid = value;
      fieldSetFlags()[6] = true;
      return this;
    }

    /**
      * Checks whether the 'Openudid' field has been set.
      * @return True if the 'Openudid' field has been set, false otherwise.
      */
    public boolean hasOpenudid() {
      return fieldSetFlags()[6];
    }


    /**
      * Clears the value of the 'Openudid' field.
      * @return This builder.
      */
    public Builder clearOpenudid() {
      Openudid = null;
      fieldSetFlags()[6] = false;
      return this;
    }

    /**
      * Gets the value of the 'Uid' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getUid() {
      return Uid;
    }

    /**
      * Sets the value of the 'Uid' field.
      * @param value The value of 'Uid'.
      * @return This builder.
      */
    public Builder setUid(java.util.Map<String,Integer> value) {
      validate(fields()[7], value);
      this.Uid = value;
      fieldSetFlags()[7] = true;
      return this;
    }

    /**
      * Checks whether the 'Uid' field has been set.
      * @return True if the 'Uid' field has been set, false otherwise.
      */
    public boolean hasUid() {
      return fieldSetFlags()[7];
    }


    /**
      * Clears the value of the 'Uid' field.
      * @return This builder.
      */
    public Builder clearUid() {
      Uid = null;
      fieldSetFlags()[7] = false;
      return this;
    }

    /**
      * Gets the value of the 'Did' field.
      * @return The value.
      */
    public java.util.Map<String,Integer> getDid() {
      return Did;
    }

    /**
      * Sets the value of the 'Did' field.
      * @param value The value of 'Did'.
      * @return This builder.
      */
    public Builder setDid(java.util.Map<String,Integer> value) {
      validate(fields()[8], value);
      this.Did = value;
      fieldSetFlags()[8] = true;
      return this;
    }

    /**
      * Checks whether the 'Did' field has been set.
      * @return True if the 'Did' field has been set, false otherwise.
      */
    public boolean hasDid() {
      return fieldSetFlags()[8];
    }


    /**
      * Clears the value of the 'Did' field.
      * @return This builder.
      */
    public Builder clearDid() {
      Did = null;
      fieldSetFlags()[8] = false;
      return this;
    }

    public IDs build() {
      try {
        IDs record = new IDs();
        record.Global_Id = fieldSetFlags()[0] ? this.Global_Id : (String) defaultValue(fields()[0]);
        record.Imei = fieldSetFlags()[1] ? this.Imei : (java.util.Map<String,Integer>) defaultValue(fields()[1]);
        record.Mac = fieldSetFlags()[2] ? this.Mac : (java.util.Map<String,Integer>) defaultValue(fields()[2]);
        record.Imsi = fieldSetFlags()[3] ? this.Imsi : (java.util.Map<String,Integer>) defaultValue(fields()[3]);
        record.Phone_Number = fieldSetFlags()[4] ? this.Phone_Number : (java.util.Map<String,Integer>) defaultValue(fields()[4]);
        record.Idfa = fieldSetFlags()[5] ? this.Idfa : (java.util.Map<String,Integer>) defaultValue(fields()[5]);
        record.Openudid = fieldSetFlags()[6] ? this.Openudid : (java.util.Map<String,Integer>) defaultValue(fields()[6]);
        record.Uid = fieldSetFlags()[7] ? this.Uid : (java.util.Map<String,Integer>) defaultValue(fields()[7]);
        record.Did = fieldSetFlags()[8] ? this.Did : (java.util.Map<String,Integer>) defaultValue(fields()[8]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
