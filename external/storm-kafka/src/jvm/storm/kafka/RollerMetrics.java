package storm.kafka;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RollerMetrics {
	
    private static class RollerMetric
    {
    	Double _summed_values = 0d;
    	int _count_of_data_points = 0;
    	Date _dateCreated;
    	int _minuteSinceStartThisRepresents;
    	public RollerMetric (int representingMinuteSinceStart)
    	{
    		_minuteSinceStartThisRepresents = representingMinuteSinceStart;
    		_dateCreated = new Date();
    	}
    	
    	public void report_current_value (double value)
    	{
    		_summed_values = _summed_values + value;
    		_count_of_data_points = _count_of_data_points + 1;
    	}
    	public Date date_metric_created() { return _dateCreated; }
    	public int minuteSinceStartThisRepresents() { return _minuteSinceStartThisRepresents; }
    }	
	
    int _totalSlots;
    int _last_List_index;
    int _last_minute_as_slot_zero;
    Date _date_metrics_begun;
    LinkedList<RollerMetric> _summedValuesPerMinute;
    Boolean _debugOutput;
    
    public RollerMetrics (Boolean debugOutput) 
    {
    	_debugOutput = debugOutput;
    	if ( debugOutput )
    	{
    		_totalSlots = 12;	//12 minutes
    	}
    	else
    	{
    		_totalSlots = 1440;	//24 hours
    	}
    	
    	_last_List_index = _totalSlots - 1;
    	_last_minute_as_slot_zero = 0;
    	_date_metrics_begun = new Date();
    	_summedValuesPerMinute = new LinkedList<RollerMetric>(); 
    	_summedValuesPerMinute.add(0, new RollerMetric(0));
    	/*
    	for (int i=1; i < _totalSlots; i++)  // causes all of the metrics to report on first pass
    	{
    		_summedValuesPerMinute.add(i, new RollerMetric(0));
    	}    
    	*/	
    }
    
    public void report_current_value(double value)
    {
    	addNewMinutesToBottom();
    	_summedValuesPerMinute.get(0).report_current_value(value);	
    }
    List<Integer> _minutes_to_sum_up_to;
    private List<Integer> getMinutesToSumUp()
    {
    	if ( _minutes_to_sum_up_to == null )
		{
			_minutes_to_sum_up_to = new ArrayList<Integer>();
			_minutes_to_sum_up_to.add(1);
			_minutes_to_sum_up_to.add(5);

	    	if (_debugOutput)
	    	{
	    		_minutes_to_sum_up_to.add(0);
	    		_minutes_to_sum_up_to.add(2);
	    		_minutes_to_sum_up_to.add(3);
	    		_minutes_to_sum_up_to.add(4);
	    	}
	    	else
	    	{
	    		_minutes_to_sum_up_to.add(10);
	    		_minutes_to_sum_up_to.add(30);
	    		_minutes_to_sum_up_to.add(60);		// 1 hour
	    		_minutes_to_sum_up_to.add(120);		// 2 hour
	    		_minutes_to_sum_up_to.add(360);		// 3 hour
	    		_minutes_to_sum_up_to.add(1440);		// 24 hour
	    	}			
		}

    	return _minutes_to_sum_up_to;
    }
    
    public HashMap<String, Double> getMetrics ()
    {
        HashMap<String, Double> ret = new HashMap<String, Double>();
    	
        List<Integer> minutes_to_sum_up_to = getMinutesToSumUp();

    	double summedValue = 0;
    	int dataPoints = 0;
    	Date lastDateSeen = new Date();
    	int lastMinuteRepresented = _last_minute_as_slot_zero + 1;
    	for (int i=0; i < _summedValuesPerMinute.size(); i++)
    	{
    		RollerMetric m = _summedValuesPerMinute.get(i);
    		
    		Date this_metrics_earliest_date = m.date_metric_created();
    		assert  this_metrics_earliest_date.before(lastDateSeen);
    		lastDateSeen = this_metrics_earliest_date;
    		
    		int representsThisMinute = m.minuteSinceStartThisRepresents();
    		assert (lastMinuteRepresented - 1) == representsThisMinute ;
    		assert ( _last_minute_as_slot_zero - i ) == representsThisMinute;
    		lastMinuteRepresented = representsThisMinute;
    		
    		
    		double this_minutes_value = m._summed_values;
    		int this_minutes_data_points = m._count_of_data_points;
    		summedValue = summedValue + this_minutes_value;
    		dataPoints = dataPoints + this_minutes_data_points;
    		
    		if (minutes_to_sum_up_to.contains(i))
    		{
    			ret.putAll(buildMetricsOverFirstXMinutes(i, summedValue, dataPoints, representsThisMinute,this_minutes_value,  this_minutes_data_points ));
    		}
        	if (_debugOutput)
        	{
        		System.out.println("index=" + i + ".  this_minute=" + representsThisMinute + ".  total_dataPoints=" + dataPoints + ".  total_summedValue=" + summedValue + ".  this_minutes_dataPoints=" + this_minutes_data_points + ".  this_minutes_value=" + this_minutes_value + ".");
        	}
    	}
    	
    	if (_debugOutput)
    	{
        	ret.put("/in_minute/totalMinutesSinceFirstMetric/value", (double)totalMinutesSinceFirstMetric());
        	ret.put("/in_minute/_last_minute_as_slot_zero/value", (double)_last_minute_as_slot_zero);
    		if (_summedValuesPerMinute.size() >= 1)
    		{
	        	ret.put("/in_minute/_slot_0/_summed_values/value", _summedValuesPerMinute.get(0)._summed_values);
	        	ret.put("/in_minute/_slot_0/_count_data_points/value", (double)_summedValuesPerMinute.get(0)._count_of_data_points);
	        	
	    		if (_summedValuesPerMinute.size() >= 2)
	    		{
		        	ret.put("/in_minute/_slot_1/_summed_values/value", _summedValuesPerMinute.get(1)._summed_values);
		        	ret.put("/in_minute/_slot_1/_count_data_points/value", (double)_summedValuesPerMinute.get(1)._count_of_data_points);
		        	
		    		if (_summedValuesPerMinute.size() >= 3)
		    		{
			        	ret.put("/in_minute/_slot_2/_summed_values/value", _summedValuesPerMinute.get(2)._summed_values);
			        	ret.put("/in_minute/_slot_2/_count_data_points/value", (double)_summedValuesPerMinute.get(2)._count_of_data_points);     
		    		}	        	
	    		}	        	
    		}
    	}

    	return ret;

    }
    
    private HashMap<String, Double> buildMetricsOverFirstXMinutes(int firstMinutes, double summed_value, double count_of_data_points, double representsThisMinute, double this_minutes_value,  double this_minutes_data_points)
    {
    	HashMap<String, Double> ret = new HashMap<String, Double>();
    	String metric_path_pre = "/in_minute/" + String.format("%04d", firstMinutes);
    	
    	if (_debugOutput)
    	{
    		ret.put(metric_path_pre + "/represents_minute/value", representsThisMinute);
    	  	ret.put(metric_path_pre + "/summed/rolling/value", summed_value);
        	ret.put(metric_path_pre + "/data_point_count/rolling/value", count_of_data_points);
    	  	ret.put(metric_path_pre + "/summed/this_minute_only/value", this_minutes_value);
        	ret.put(metric_path_pre + "/data_point_count/this_minute_only/value", this_minutes_data_points);
    	}

    	if ( count_of_data_points > 0 ) 
    		{ 
    			double avg_value = summed_value / count_of_data_points ; 
    	    	ret.put(metric_path_pre + "/avg/value", avg_value);
    		};
    		
    	double first_milliseconds = (double)firstMinutes * 60d * 1000d;
    	double value_per_millisecond = summed_value / first_milliseconds ;
    	ret.put(metric_path_pre + "/per_millisecond/value", value_per_millisecond);

    	return ret;
    }
    
    private long totalMinutesSinceFirstMetric()
    {
    	Date thisUpdate = new Date();
    	long totalMillisecondsSinceFirstMetric = thisUpdate.getTime() - _date_metrics_begun.getTime();
    	long totalMinutesSinceFirstMetric = TimeUnit.MILLISECONDS.toMinutes(totalMillisecondsSinceFirstMetric); 
    	return totalMinutesSinceFirstMetric;
    }
    
    private void addNewMinutesToBottom()
    {
    	// we want to actually gauge the # of time that has passed.
    	//	it is possible that we get zero data points within a minute.
    	while ( _last_minute_as_slot_zero < totalMinutesSinceFirstMetric() )
    	{
        	if ( _summedValuesPerMinute.size() >= _totalSlots)
        	{
        		assert  _summedValuesPerMinute.size() == _totalSlots ;
        		_summedValuesPerMinute.remove(_last_List_index);
        	}
        	
        	_last_minute_as_slot_zero = _last_minute_as_slot_zero + 1;
        	
        	_summedValuesPerMinute.add(0, new RollerMetric(_last_minute_as_slot_zero)); // shifts the previous "0" minute up to the index 1 position

    	}
    }    
  
    
    
	public static void main(String[] args) throws InterruptedException 
	{

		RollerMetrics r = new RollerMetrics(true);
		
		for (int i = 0; i < 40 ; i++)
		{
			r.report_current_value(1);

			if ( i < 3 )
			{
				Thread.sleep(10);
			}
			else
			{
				Thread.sleep(10000);
			}
			
			HashMap<String, Double> ret =  r.getMetrics();
			System.out.println(ret);
			System.out.println("i=" + i + ".");
			System.out.println("===========================");
		}

		HashMap<String, Double> ret =  r.getMetrics();

		System.out.println(ret);
		
	}    
    
}
