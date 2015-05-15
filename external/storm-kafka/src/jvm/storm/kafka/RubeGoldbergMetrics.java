package storm.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RubeGoldbergMetrics {
    
	private SortedMap<String,RubeGoldbergMetric> _metrics = new TreeMap<String,RubeGoldbergMetric>();
	
	public RubeGoldbergMetrics(){}
	
	// I like to see the metric exist right away, not wait until it has been observed once
	public void initialize(String metricPath)
	{
		getMetric(metricPath);
	}
	// I like to see the metric exist right away, not wait until it has been observed once
	public void initialize(String metricClassification, String metricPath)
	{
		getMetric(metricClassification,metricPath);
	}
	
	public void turnOnRollingMetrics(String metricPath)
	{
		getMetric(metricPath).turnOnRollingMetrics();
	}	
	
	public void turnOnRollingMetrics(String metricClassification, String metricPath)
	{
		getMetric(metricClassification,metricPath).turnOnRollingMetrics();
	}
	
    public void incr(String metricPath) {
    	getMetric(metricPath).incr();
    }
    public void incr(String metricClassification, String metricPath) {
    	getMetric(metricClassification,metricPath).incr();
    }
    
    public void incrBy(String metricPath, double incrementBy) {
    	getMetric(metricPath).incrBy(incrementBy);
    }
    public void incrBy(String metricClassification, String metricPath, double incrementBy) {
    	getMetric(metricClassification,metricPath).incrBy(incrementBy);
    }
    
    public void setGaugesCurrentValue(String metricPath, double value){
    	getMetric(metricPath).setGaugesCurrentValue(value);
    }
    public void setGaugesCurrentValue(String metricClassification, String metricPath, double value){
    	getMetric(metricClassification, metricPath).setGaugesCurrentValue(value);
    }    
    
    public void setTotalRunningValue(String metricPath, double value){
    	getMetric(metricPath).setTotalRunningValue(value);
    } 
    
    public void setTotalRunningValue(String metricClassification, String metricPath, double value){
    	getMetric(metricClassification, metricPath).setTotalRunningValue(value);
    }     
    
    public HashMap<String, Double> getMetricsAndReset() {
		HashMap<String, Double> ret = new HashMap<String, Double>();
		
		for (Map.Entry<String, RubeGoldbergMetric> entry: _metrics.entrySet())
		{
			RubeGoldbergMetric _rubyMetric = entry.getValue();
			ret.putAll(_rubyMetric.getMetricsAndReset());
		}
		
		return ret;
    }
    
    public HashMap<String, Double> getMetricsCurrentValueOnly() {
		HashMap<String, Double> ret = new HashMap<String, Double>();
		
		for (Map.Entry<String, RubeGoldbergMetric> entry: _metrics.entrySet())
		{
			RubeGoldbergMetric _rubyMetric = entry.getValue();
			ret.put(_rubyMetric.getMetricPath(), _rubyMetric.getCurrentValue());
		}
		
		return ret;
    }    
    
    public List<RubeGoldbergMetric> getRubyMetrics() 
    {
    	List<RubeGoldbergMetric> ret = new ArrayList<RubeGoldbergMetric>();
		for (Map.Entry<String, RubeGoldbergMetric> entry: _metrics.entrySet())
		{
			RubeGoldbergMetric _rubyMetric = entry.getValue();
			ret.add(_rubyMetric);
		}
		return ret;
	}
    
    public List<RubeGoldbergMetric> getRubyMetricsXMetricClassification(String metricClassification)
    {
    	List<RubeGoldbergMetric> ret = new ArrayList<RubeGoldbergMetric>();
		for (Map.Entry<String, RubeGoldbergMetric> entry: _metrics.entrySet())
		{
			RubeGoldbergMetric _rubyMetric = entry.getValue();
			if (metricClassification.equalsIgnoreCase(_rubyMetric.getMetricClassification()))
			{
				ret.add(_rubyMetric);
			}
		}
		return ret;
    }
    
    
    private RubeGoldbergMetric getMetric(String metricPath) {
    	return ( getMetric("unspecified", metricPath) );
    }
    
    private RubeGoldbergMetric getMetric(String metricClassification, String metricPath) {
    	if (! _metrics.containsKey(metricPath))
    	{
    		_metrics.put(metricPath, new RubeGoldbergMetric(metricClassification, metricPath));
    	}
    	return ( _metrics.get(metricPath) );
    }    
}
