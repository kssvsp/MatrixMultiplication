import java.io.IOException;  
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class matrixmul {
public static int m1r=0;
public static int m2c=0;
public static List<String> mi1 = new ArrayList<String>();
public static List<String> mi2 = new ArrayList<String>();
public static class map_matrix
extends Mapper < LongWritable, Text, Text, IntWritable >
{
private final static IntWritable o = new IntWritable( 1);
private Text w = new Text();

@Override
public void map( LongWritable key, Text value, Context context
) throws IOException,
InterruptedException
{
	String newline = "\n";
	
	if(!(String.valueOf(value).contains(newline)))
		System.out.println("Not a new line");
	else
		mi1.add(" ");
	if(!(String.valueOf(mi1).contains(", ]")))
		mi1.add(String.valueOf(value));
	else
		mi2.add(String.valueOf(value));


   if(!(String.valueOf(mi1).split(",")[0].split("\\s").length==String.valueOf(mi2).split(",").length))
   {
	   System.out.println("Cannot multiply the matrices");
   }
   else
   {

  List<Integer> map_op = new ArrayList<Integer>();
  int p=1;

	String m1[] = String.valueOf(mi1).replace("[", "").replace("]", "").split(",");
	m1r = (String.valueOf(mi1).replace("[", "").replace("]", "").split(",").length-1);

	String m2[] = String.valueOf(mi2).replace("[", "").replace("]", "").split(",");
	int m2r = String.valueOf(mi2).replace("[", "").replace("]", "").split(",").length;
	m2c = String.valueOf(mi2).replace("[", "").replace("]", "").split(",")[0].split("\\s").length;

	for(int k=0;k<m1r;k++)
	{
		String mm[] = m1[k].trim().split("\\s");
		for(int j=0;j<m2c;j++)
		{
			for(int i=0;i<m2r;i++)
			{
				map_op.add(Integer.parseInt(mm[i])*Integer.parseInt(m2[i].trim().split("\\s")[j]));
				o.set(Integer.parseInt(mm[i])*Integer.parseInt(m2[i].trim().split("\\s")[j]));
				w.set(String.valueOf(p));
				context.write( w,o);
			}
			p = p+1;
		}
	}

}
}
}
public static class reduce_matrix
extends
Reducer < Text, IntWritable, Text, IntWritable > {
public ArrayList<String> Result = new ArrayList<String>();
private IntWritable op = new IntWritable();
@Override
public void reduce( Text key, Iterable < IntWritable > values, Context context
) throws IOException,
InterruptedException
{
	int s = 0;
	for (IntWritable val : values)
	{
		s = s + val.get();
	}
	op.set( s);

	Result.add(" "+key+":"+String.valueOf(op));
	String op_t[] = String.valueOf(Result).replace("[", "").replace("]", "").split(",");

	int t = 1;
	if(String.valueOf(op_t.length).equalsIgnoreCase(String.valueOf(m1r*m2c)))
	{
		for(int a=0;a<m1r;a++)
		{
			String q="";
			for(int b=0;b<m2c;b++)
			{
				String m="";
				for(int c=0;c<op_t.length;c++)
				{

					if(!(op_t[c].contains(" "+t+":")))
					{
						System.out.println("op_t[c] does not contain t");
					}
					else
					{

						m=op_t[c].split(":")[1];
						break;
					}
				}
				q = q + m +" ";
				t++;
			}

			context.write(new Text(q.trim()),null);
		}
	}
	}
}

public static void main( String[] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = Job.getInstance( conf, "matrix mul");
	job.setJarByClass(matrixmul.class);

	FileInputFormat.addInputPath( job, new Path("input"));
	FileOutputFormat.setOutputPath( job, new Path("output"));
	job.setMapperClass( map_matrix.class);
	job.setReducerClass( reduce_matrix.class);

	job.setOutputKeyClass( Text.class);
	job.setOutputValueClass( IntWritable.class);

	System.exit( job.waitForCompletion( true) ? 0 : 1);
	}
}


