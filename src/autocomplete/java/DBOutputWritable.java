package autocomplete.java;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DBOutputWritable implements Writable, DBWritable{

	private String starting_phrase;
	private String following_word;
	private int appearance;
	public DBOutputWritable(String starting_phrase, String following_word, int appearance) {
//		super();
		this.starting_phrase = starting_phrase;
		this.following_word = following_word;
		this.appearance = appearance;
	}
	@Override
	public void readFields(ResultSet arg0) throws SQLException {
		// TODO Auto-generated method stub
		this.starting_phrase = arg0.getString(1);
		this.following_word = arg0.getString(2);
		this.appearance = arg0.getInt(3);
	}
	@Override
	public void write(PreparedStatement arg0) throws SQLException {
		// TODO Auto-generated method stub
		arg0.setString(1, starting_phrase);
		arg0.setString(2, following_word);
		arg0.setInt(3, appearance);
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	
}
