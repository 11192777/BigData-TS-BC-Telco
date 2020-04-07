package pers.qingyu.app.kvbean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import pers.qingyu.app.base.BaseDemension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class CommonDemension extends BaseDemension {

	private ContactDemension contactDemension = new ContactDemension();
	private DateDemension dateDemension = new DateDemension();

	@Override
	public int compareTo(Object o) {
		CommonDemension another = (CommonDemension) o;

		int result = contactDemension.compareTo(another.contactDemension);
		if (result == 0) {
			result = dateDemension.compareTo(another.dateDemension);
		}

		return result;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.contactDemension.write(dataOutput);
		this.dateDemension.write(dataOutput);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.contactDemension.readFields(dataInput);
		this.dateDemension.readFields(dataInput);
	}

	@Override
	public String toString() {
		return contactDemension.toString() + "\t" + dateDemension.toString();
	}
}
