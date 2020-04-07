package pers.qingyu.app.kvbean;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import pers.qingyu.app.base.BaseDemension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@NoArgsConstructor
@Getter
@Setter
public class ContactDemension extends BaseDemension {

	private String phoneNumber;
	private String name;


	@Override
	public int compareTo(Object o) {
		ContactDemension another = (ContactDemension) o;
		return this.phoneNumber.compareTo(another.phoneNumber);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.phoneNumber);
		dataOutput.writeUTF(this.name);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.phoneNumber = dataInput.readUTF();
		this.name = dataInput.readUTF();
	}

	@Override
	public String toString(){
		return phoneNumber + "\t" + name;
	}
}
