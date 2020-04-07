package pers.qingyu.app.base;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import javax.ws.rs.GET;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class BaseValue implements Writable {

}
