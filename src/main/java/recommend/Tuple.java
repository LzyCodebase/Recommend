package recommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Tuple implements WritableComparable<Tuple> {
    private Text key ;
    private Text item ;
    private Text value ;

    public Tuple(){
        key = new Text();
        item = new Text();
        value = new Text();
    }
    public Tuple(Tuple tuple){
        this.key = new Text(tuple.getKey().toString());
        this.item = new Text(tuple.getItem().toString());
        this.value = new Text(tuple.getValue().toString());
    }
    public Tuple(String k,String i,String v){
        this.key = new Text(k);
        this.item = new Text(i);
        this.value = new Text(v);
    }
    public Tuple(Text key ,Text item ,Text value){
        this(key.toString(),item.toString(),value.toString());
    }
    public void set(String k,String i,String v){
        this.key = new Text(k);
        this.item = new Text(i);
        this.value = new Text(v);
    }
    public void set(Tuple tuple){
        set(tuple.getKey().toString(),tuple.getItem().toString(),tuple.getValue().toString());
    }
    public void set(Text key ,Text item ,Text value){
        set(key.toString(),item.toString(),value.toString());
    }

    @Override
    public int compareTo(Tuple o) {
        if (key.compareTo(o.key)!=0){
            return key.compareTo(o.key);
        }else if (item.compareTo(o.item)!=0){
            return (-1)*item.compareTo(o.item);
        }else {
            return value.compareTo(o.value);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
        item.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
        item.readFields(in);
        value.readFields(in);
    }

    public Text getKey() {
        return key;
    }
    public Text getItem() {
        return item;
    }
    public Text getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple tuple = (Tuple) o;

        if (key != null ? !key.equals(tuple.key) : tuple.key != null) return false;
        if (item != null ? !item.equals(tuple.item) : tuple.item != null) return false;
        return value != null ? value.equals(tuple.value) : tuple.value == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return  key.toString()+","+item.toString()+ "\t" + value.toString() ;
    }
}
