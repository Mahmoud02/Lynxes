package org.mahmoud.fastqueue.server.ui.views;

import com.vaadin.flow.component.html.Div;
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
 * Main dashboard view showing FastQueue2 overview and key metrics
 */
@Route(value = "", layout = MainLayout.class)
@PageTitle("Dashboard | FastQueue2")
public class DashboardView extends VerticalLayout {
    
    public DashboardView() {
        addClassNames(LumoUtility.Padding.LARGE);
        
        H1 title = new H1("FastQueue2 Dashboard");
        title.addClassNames(LumoUtility.FontSize.XXXLARGE, LumoUtility.Margin.Bottom.LARGE);
        
        // Server Status Card
        VerticalLayout statusCard = createStatusCard();
        
        // Quick Stats Cards
        HorizontalLayout statsLayout = new HorizontalLayout();
        statsLayout.setWidthFull();
        statsLayout.add(createStatCard("Topics", "5", VaadinIcon.LIST, "Total topics"));
        statsLayout.add(createStatCard("Messages", "1,234", VaadinIcon.ENVELOPE, "Total messages"));
        statsLayout.add(createStatCard("Consumers", "12", VaadinIcon.USERS, "Active consumers"));
        statsLayout.add(createStatCard("Throughput", "500/s", VaadinIcon.TRENDING_UP, "Messages per second"));
        
        // Recent Activity
        VerticalLayout activityCard = createActivityCard();
        
        add(title, statusCard, statsLayout, activityCard);
    }
    
    private VerticalLayout createStatusCard() {
        VerticalLayout card = new VerticalLayout();
        card.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                          LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 cardTitle = new H2("Server Status");
        cardTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        HorizontalLayout statusLayout = new HorizontalLayout();
        Icon statusIcon = VaadinIcon.CHECK_CIRCLE.create();
        statusIcon.addClassNames(LumoUtility.TextColor.SUCCESS);
        Span statusText = new Span("Server is running");
        statusText.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontWeight.BOLD);
        statusLayout.add(statusIcon, statusText);
        
        Span uptime = new Span("Uptime: 2 days, 14 hours, 32 minutes");
        Span port = new Span("Port: 8080");
        
        card.add(cardTitle, statusLayout, uptime, port);
        return card;
    }
    
    private VerticalLayout createStatCard(String title, String value, VaadinIcon icon, String description) {
        VerticalLayout card = new VerticalLayout();
        card.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                          LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        card.setAlignItems(Alignment.CENTER);
        
        Icon cardIcon = icon.create();
        cardIcon.addClassNames(LumoUtility.TextColor.PRIMARY, LumoUtility.FontSize.LARGE);
        
        Span valueSpan = new Span(value);
        valueSpan.addClassNames(LumoUtility.FontSize.XXXLARGE, LumoUtility.FontWeight.BOLD, 
                               LumoUtility.TextColor.PRIMARY);
        
        Span titleSpan = new Span(title);
        titleSpan.addClassNames(LumoUtility.FontWeight.BOLD);
        
        Span descSpan = new Span(description);
        descSpan.addClassNames(LumoUtility.TextColor.SECONDARY, LumoUtility.FontSize.SMALL);
        
        card.add(cardIcon, valueSpan, titleSpan, descSpan);
        return card;
    }
    
    private VerticalLayout createActivityCard() {
        VerticalLayout card = new VerticalLayout();
        card.addClassNames(LumoUtility.Border.ALL, LumoUtility.BorderRadius.MEDIUM, 
                          LumoUtility.Padding.MEDIUM, LumoUtility.Background.BASE);
        
        H2 cardTitle = new H2("Recent Activity");
        cardTitle.addClassNames(LumoUtility.Margin.Top.NONE, LumoUtility.Margin.Bottom.MEDIUM);
        
        // Mock recent activity
        String[] activities = {
            "Topic 'orders' created",
            "Message published to 'notifications'",
            "Consumer connected to 'events'",
            "Topic 'logs' deleted",
            "Health check performed"
        };
        
        for (String activity : activities) {
            HorizontalLayout activityItem = new HorizontalLayout();
            Icon icon = VaadinIcon.CIRCLE.create();
            icon.addClassNames(LumoUtility.TextColor.SUCCESS, LumoUtility.FontSize.SMALL);
            Span text = new Span(activity);
            activityItem.add(icon, text);
            activityItem.setAlignItems(Alignment.CENTER);
            card.add(activityItem);
        }
        
        return card;
    }
}
