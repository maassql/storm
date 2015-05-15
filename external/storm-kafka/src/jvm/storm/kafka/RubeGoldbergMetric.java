package storm.kafka;

import java.util.HashMap;
import java.util.Map;

public class RubeGoldbergMetric {
    double _gauge_value = 0;
    double _total_running_value = 0;
    double _gauge_value_last_reported = 0;
    double _total_value_last_reported = 0;
    String _metric_Path = "";
    String _metric_Classification = "";
    boolean _this_is_a_guage = false;
    boolean _this_is_a_counter = false;
    boolean _collectRollingMetrics = false;
    RollerMetrics _roller ;
    RollerMetrics _roller_of_delta;
    
    public RubeGoldbergMetric(String metricClassification, String metricPath) {
    	_metric_Path = metricPath;
    	_metric_Classification = metricClassification;
    }
    
    public void turnOnRollingMetrics()
    {
    	_collectRollingMetrics = true;
    	if ( _roller == null )
    	{
    		_roller = new RollerMetrics(true);
    		_roller_of_delta = new RollerMetrics(true);
    	}
    }
    
    public void incr() {
    	_total_running_value++;
    }

    public void incrBy(double incrementBy) {
    	_total_running_value += incrementBy;
    }
    
    public void setGaugesCurrentValue(double value){
    	_gauge_value = value;
    	_this_is_a_guage = true;
    }
    
    public void setTotalRunningValue(double value){
    	_total_running_value = value;
    } 

    public HashMap<String, Double> getMetricsAndReset() {
        HashMap<String, Double> ret = new HashMap<String, Double>();

        String metricPre;
        double value;
        double delta_value;
        double last_reported_value;
        if ( _this_is_a_guage )
        {
        	value = _gauge_value;
        	last_reported_value = _gauge_value_last_reported;
        	metricPre = _metric_Path + "/gauge";
        	_gauge_value_last_reported = value;
        }
        else
        {
        	value = _total_running_value;
        	last_reported_value = _total_value_last_reported;
        	metricPre =  _metric_Path + "/total";
        	_total_value_last_reported = value;
        }
        
        delta_value = value - last_reported_value;
    	ret.put(metricPre + "/value", value);
    	ret.put(metricPre + "/delta/value", delta_value);
        
        if (_collectRollingMetrics)
    	{
        	_roller.report_current_value(value);
        	
        	for ( Map.Entry<String, Double> e : _roller.getMetrics().entrySet())
        	{
        		ret.put(metricPre + "/" + e.getKey(), e.getValue());
        	}
        	
        	_roller_of_delta.report_current_value(delta_value);
        	
        	for ( Map.Entry<String, Double> e : _roller_of_delta.getMetrics().entrySet())
        	{
        		ret.put(metricPre + "/delta" + e.getKey(), e.getValue());
        	}        	
	
    	}
        
        _gauge_value_last_reported = _gauge_value;
        
        
        return ret;
    }
   
    
    public double getCurrentValue()
    { 
        if ( _this_is_a_guage )
        {
            return _gauge_value ;
        }
        else
        {
    		return _total_running_value;
        }
    }
    public String getMetricClassification() {return(this._metric_Classification);}
    public String getMetricPath() { return this._metric_Path ; }
    public void setMetricPath(String newPath) { this._metric_Path = newPath; }
    

    
    
}