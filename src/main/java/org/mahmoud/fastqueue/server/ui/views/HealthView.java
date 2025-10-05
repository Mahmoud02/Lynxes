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
 * Health check view showing server health status and diagnostics
 */
@Route(value = "health", layout = MainLayout.class)
@PageTitle("Health | FastQueue2")
public class HealthView extends VerticalLayout {
    
    public HealthView() {
        addClassNames(LumoUtility.Padding.LARGE);
        
        H1 title = new H1("Health Status");
        title.addClassNames(LumoUtility.FontSize.XXXLARGE, LumoUtility.Margin.Bottom.LARGE);
        
        // Overall Health Status
        VerticalLayout healthSection = createHealthSection();
        
        // Component Health
        VerticalLayout componentSection = createComponentSection();
        
        // System Health
        VerticalLayout systemSection = createSystemSection();
        
        add(title, healthSection, componentSection, systemSection);
    }
    
    private VerticalLayout createHealthSection() {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("Overall Health");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        HorizontalLayout healthLayout = new HorizontalLayout();
        healthLayout.setAlignItems(Alignment.CENTER);
        
        Icon healthIcon = VaadinIcon.CHECK_CIRCLE.create();
        healthIcon.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontSize.LARGE);
        
        Span healthStatus = new Span("Server is healthy");
        healthStatus.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontWeight.BOLD, 
                                 LumoUtility.FontSize.LARGE);
        
        Span uptime = new Span("Uptime: 2 days, 14 hours, 32 minutes");
        uptime.addClassNames(LumoUtility.TextColor.SECONDARY);
        
        healthLayout.add(healthIcon, healthStatus);
        
        VerticalLayout healthInfo = new VerticalLayout(healthLayout, uptime);
        healthInfo.setPadding(false);
        
        section.add(sectionTitle, healthInfo);
        return section;
    }
    
    private VerticalLayout createComponentSection() {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("Component Health");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        // Mock component health data
        String[] components = {"HTTP Server", "Message Store", "Topic Registry", "Metrics Collector", "Health Monitor"};
        VaadinIcon[] icons = {VaadinIcon.SERVER, VaadinIcon.STORAGE, VaadinIcon.LIST, VaadinIcon.CHART, VaadinIcon.HEART};
        
        for (int i = 0; i < components.length; i++) {
            HorizontalLayout componentRow = new HorizontalLayout();
            componentRow.setWidthFull();
            componentRow.setAlignItems(Alignment.CENTER);
            
            Icon componentIcon = icons[i].create();
            componentIcon.addClassNames(LumoUtility.TextColor.SUCCESS);
            
            Span componentName = new Span(components[i]);
            componentName.addClassNames(LumoUtility.FontWeight.BOLD);
            
            Icon statusIcon = VaadinIcon.CHECK_CIRCLE.create();
            statusIcon.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontSize.SMALL);
            
            Span statusText = new Span("Healthy");
            statusText.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontSize.SMALL);
            
            componentRow.add(componentIcon, componentName, statusIcon, statusText);
            componentRow.expand(componentName);
            
            section.add(componentRow);
        }
        
        return section;
    }
    
    private VerticalLayout createSystemSection() {
        VerticalLayout section = new VerticalLayout();
        section.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                             LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 sectionTitle = new H2("System Health");
        sectionTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        HorizontalLayout metricsLayout = new HorizontalLayout();
        metricsLayout.setWidthFull();
        
        metricsLayout.add(createHealthMetricCard("CPU", "23%", VaadinIcon.DESKTOP, "Normal"));
        metricsLayout.add(createHealthMetricCard("Memory", "512 MB", VaadinIcon.STORAGE, "Normal"));
        metricsLayout.add(createHealthMetricCard("Disk", "2.1 GB", VaadinIcon.HARDDRIVE, "Normal"));
        metricsLayout.add(createHealthMetricCard("Network", "127 conn", VaadinIcon.CONNECT, "Normal"));
        
        section.add(sectionTitle, metricsLayout);
        return section;
    }
    
    private VerticalLayout createHealthMetricCard(String title, String value, VaadinIcon icon, String status) {
        VerticalLayout card = new VerticalLayout();
        card.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.SMALL, 
                          LumoUtility.Padding.SMALL, LumoUtility.Background.CONTRAST_5);
        card.setAlignItems(Alignment.CENTER);
        
        Icon cardIcon = icon.create();
        cardIcon.addClassNames(LumoUtility.TextColor.PRIMARY, LumoUtility.FontSize.LARGE);
        
        Span valueSpan = new Span(value);
        valueSpan.addClassNames(LumoUtility.FontSize.LARGE, LumoUtility.FontWeight.BOLD, 
                               LumoUtility.TextColor.PRIMARY);
        
        Span titleSpan = new Span(title);
        titleSpan.addClassNames(LumoUtility.FontWeight.BOLD);
        
        Span statusSpan = new Span(status);
        statusSpan.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontSize.SMALL);
        
        card.add(cardIcon, valueSpan, titleSpan, statusSpan);
        return card;
    }
}
