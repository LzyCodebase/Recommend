package recommend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBTuple implements WritableComparable<DBTuple>, DBWritable {
    private Text user;
    private Text item;
    private Text target;

    public DBTuple(){
        user = new Text();
        item = new Text();
        target = new Text();
    }
    public  DBTuple(DBTuple DBTuple){
        user = new Text(DBTuple.getuser());
        item = new Text(DBTuple.getitem());
        target = new Text(DBTuple.gettarget());
    }
    public DBTuple(String u ,String i ,String t){
        user = new Text(u);
        item = new Text(i);
        target = new Text(t);
    }
    public DBTuple(Text user ,Text item ,Text target){
        this(user.toString(),item.toString(),target.toString());
    }

    public Text getuser() { return user; }

    public Text getitem() { return item; }

    public Text gettarget() { return target; }

    public void set(String u ,String i ,String t){
        user = new Text(u);
        item = new Text(i);
        target = new Text(t);
    }
    public void set(DBTuple DBTuple){
        this.set(DBTuple.getuser().toString(),DBTuple.getitem().toString(),DBTuple.gettarget().toString());
    }
    public void set(Text user ,Text item ,Text target){
        this.set(user.toString(),item.toString(),target.toString());
    }

    @Override
    public int compareTo(DBTuple o) {
        if (this.user.compareTo(o.user)!=0){
            return this.user.compareTo(o.user);
        }else if(this.target.compareTo(o.target)!=0){
            return (-1)*(this.target.compareTo(o.target));
        }else {
            return this.item.compareTo(o.item);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.user.write(out);
        this.item.write(out);
        this.target.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.user.readFields(in);
        this.item.readFields(in);
        this.target.readFields(in);
    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString( 1,this.user.toString());
        statement.setString(2,this.item.toString());
        statement.setString(3,this.target.toString());
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        user = new Text(resultSet.getString(1));
        item = new Text(resultSet.getString(2));
        target = new Text(resultSet.getString(3));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DBTuple that = (DBTuple) o;

        if (user != null ? !user.equals(that.user) : that.user != null) return false;
        if (item != null ? !item.equals(that.item) : that.item != null) return false;
        return target != null ? target.equals(that.target) : that.target == null;
    }

    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (item != null ? item.hashCode() : 0);
        result = 31 * result + (target != null ? target.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return user + "," + item + "," + target ;
    }
}
