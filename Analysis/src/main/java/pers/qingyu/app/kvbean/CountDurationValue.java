package pers.qingyu.app.kvbean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import pers.qingyu.app.base.BaseValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CountDurationValue extends BaseValue {

	private Integer count;
	private Integer duration;

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(this.count);
		dataOutput.writeInt(this.duration);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.count = dataInput.readInt();
		this.duration = dataInput.readInt();
	}

	@Override
	public String toString() {
		return count + "\t" + duration;
	}
}
