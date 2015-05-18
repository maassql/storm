package storm.kafka;

import java.util.HashMap;
import java.util.Map;

public class RubeGoldbergMetric {
    double _gauge_value = 0;
    double _total_running_value = 0;
    double _total_value_last_reported = 0;
    String _metric_Path = "";
    String _metric_Classification = "";
    boolean _this_is_a_guage = false;
    boolean _this_is_a_counter = false;
    boolean _collectRollingMetrics = false;
    RollerMetrics _roller;
    
    public RubeGoldbergMetric(String metricClassification, String metricPath) {
    	_metric_Path = metricPath;
    	_metric_Classification = metricClassification;
    }
    
    public void turnOnRollingMetrics()
    {
    	_collectRollingMetrics = true;
    	if ( _roller == null )
    	{
    		Boolean debug_roller_metric = false;
    		_roller = new RollerMetrics(debug_roller_metric);
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

        double rollup_value;
        String rollup_metric_pre;
        if ( _this_is_a_guage )
        {
        	String metricPre = _metric_Path + "/gauge";
        	ret.put(metricPre + "/value", _gauge_value); 
        	
        	rollup_value = _gauge_value;
        	rollup_metric_pre = metricPre;
        }
        else
        {
        	String metricPre =  _metric_Path + "/total";
        	String deltaMetricPre = metricPre + "/delta";
        	
            ret.put(metricPre + "/value", _total_running_value);
            
            double delta_value = _total_running_value - _total_value_last_reported;            
        	ret.put(deltaMetricPre + "/value", delta_value);  
        	
        	_total_value_last_reported = _total_running_value;
        	
        	rollup_value = delta_value;
        	rollup_metric_pre = deltaMetricPre;        	
        }

        if (_collectRollingMetrics)
    	{
        	_roller.report_current_value(rollup_value);
        	
        	for ( Map.Entry<String, Double> e : _roller.getMetrics().entrySet())
        	{
        		ret.put(rollup_metric_pre + "/" + e.getKey(), e.getValue());
        	}        	
    	}
        
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