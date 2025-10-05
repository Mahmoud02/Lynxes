package org.mahmoud.fastqueue.server.ui.views;

import com.vaadin.flow.component.html.H1;
import com.vaadin.flow.component.html.H2;
import com.vaadin.flow.component.html.Span;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.PageTitle;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.theme.lumo.LumoUtility;
import org.mahmoud.fastqueue.server.ui.layout.MainLayout;

/**
 * Metrics view showing detailed server metrics and performance data
 */
@Route(value = "metrics", layout = MainLayout.class)
@PageTitle("Metrics | FastQueue2")
public class MetricsView extends VerticalLayout {
    
    public MetricsView() {
        addClassNames(LumoUtility.Padding.LARGE);
        
        H1 title = new H1("Server Metrics");
        title.addClassNames(LumoUtility.FontSize.XXXLARGE, LumoUtility.Margin.Bottom.LARGE);
        
        // Performance Metrics
        VerticalLayout performanceSection = createPerformanceSection();
        
        // System Metrics
        VerticalLayout systemSection = createSystemSection();
        
        // Topic Metrics
        VerticalLayout topicSection = createTopicSection();
        
        add(title, performanceSection, systemSection, topicSection);
    }
    
    private VerticalLayout createPerformanceSection() {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("Performance Metrics");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        HorizontalLayout metricsLayout = new HorizontalLayout();
        metricsLayout.setWidthFull();
        
        metricsLayout.add(createMetricCard("Throughput", "1,250 msg/s", VaadinIcon.TRENDING_UP, "Messages per second"));
        metricsLayout.add(createMetricCard("Latency", "2.3 ms", VaadinIcon.CLOCK, "Average response time"));
        metricsLayout.add(createMetricCard("Queue Depth", "45", VaadinIcon.STORAGE, "Pending messages"));
        metricsLayout.add(createMetricCard("CPU Usage", "23%", VaadinIcon.DESKTOP, "Current CPU utilization"));
        
        section.add(sectionTitle, metricsLayout);
        return section;
    }
    
    private VerticalLayout createSystemSection() {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("System Metrics");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        HorizontalLayout metricsLayout = new HorizontalLayout();
        metricsLayout.setWidthFull();
        
        metricsLayout.add(createMetricCard("Memory", "512 MB", VaadinIcon.STORAGE, "Used memory"));
        metricsLayout.add(createMetricCard("Disk Usage", "2.1 GB", VaadinIcon.HARDDRIVE, "Storage used"));
        metricsLayout.add(createMetricCard("Connections", "127", VaadinIcon.CONNECT, "Active connections"));
        metricsLayout.add(createMetricCard("Threads", "45", VaadinIcon.COGS, "Active threads"));
        
        section.add(sectionTitle, metricsLayout);
        return section;
    }
    
    private VerticalLayout createTopicSection() {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("Topic Statistics");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        // Mock topic statistics
        String[] topics = {"orders", "notifications", "events", "logs", "metrics"};
        String[] messageCounts = {"1,250", "3,420", "890", "15,600", "2,100"};
        String[] publishRates = {"45/s", "120/s", "23/s", "890/s", "67/s"};
        
        for (int i = 0; i < topics.length; i++) {
            HorizontalLayout topicRow = new HorizontalLayout();
            topicRow.setWidthFull();
            topicRow.setAlignItems(Alignment.CENTER);
            
            Icon topicIcon = VaadinIcon.LIST.create();
            topicIcon.addClassNames(LumoUtility.TextColor.PRIMARY);
            
            Span topicName = new Span(topics[i]);
            topicName.addClassNames(LumoUtility.FontWeight.BOLD);
            
            Span messageCount = new Span(messageCounts[i] + " messages");
            messageCount.addClassNames(LumoUtility.TextColor.SECONDARY);
            
            Span publishRate = new Span(publishRates[i] + " publish rate");
            publishRate.addClassNames(LumoUtility.TextColor.SECONDARY);
            
            topicRow.add(topicIcon, topicName, messageCount, publishRate);
            topicRow.expand(topicName);
            
            section.add(topicRow);
        }
        
        return section;
    }
    
    private VerticalLayout createMetricCard(String title, String value, VaadinIcon icon, String description) {
        VerticalLayout card = new VerticalLayout();
        card.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.SMALL, 
                          LumoUtility.Padding.SMALL, LumoUtility.Background.CONTRAST_5);
        card.setAlignItems(Alignment.CENTER);
        
        Icon cardIcon = icon.create();
        cardIcon.addClassNames(LumoUtility.TextColor.PRIMARY, LumoUtility.FontSize.LARGE);
        
        Span valueSpan = new Span(value);
        valueSpan.addClassNames(LumoUtility.FontSize.XLARGE, LumoUtility.FontWeight.BOLD, 
                               LumoUtility.TextColor.PRIMARY);
        
        Span titleSpan = new Span(title);
        titleSpan.addClassNames(LumoUtility.FontWeight.BOLD);
        
        Span descSpan = new Span(description);
        descSpan.addClassNames(LumoUtility.TextColor.SECONDARY, LumoUtility.FontSize.SMALL);
        
        card.add(cardIcon, valueSpan, titleSpan, descSpan);
        return card;
    }
}
