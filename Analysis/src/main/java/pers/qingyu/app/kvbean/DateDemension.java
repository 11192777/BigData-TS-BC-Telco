package pers.qingyu.app.kvbean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.ws.rs.GET;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class DateDemension extends BaseDemension{
	private String year;
	private String month;
	private String day;

	@Override
	public String toString() {
		return year + "\t" + month + "\t" + day;
	}

	@Override
	public int compareTo(Object o) {
		DateDemension another = (DateDemension) o;

		int result = this.year.compareTo(another.year);
		if (result == 0) {
			result = this.month.compareTo(another.month);
			if (result == 0) {
				return  =this.day.compareTo(another.day);
			}
		}

		return result;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {

		dataOutput.writeUTF(this.year);
		dataOutput.writeUTF(this.month);
		dataOutput.writeUTF(this.day);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.year = dataInput.readUTF();
		this.month = dataInput.readUTF();
		this.day = dataInput.readUTF();
	}
}
