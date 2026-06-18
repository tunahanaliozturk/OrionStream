namespace Moongazing.OrionStream.Tests;

using System;

using Microsoft.Extensions.DependencyInjection;

using Moongazing.OrionStream;
using Moongazing.OrionStream.Diagnostics;
using Moongazing.OrionStream.Streaming;

using Xunit;

public sealed class OrionStreamRegistrationExtraTests
{
    [Fact]
    public void AddOrionStream_rejects_a_null_service_collection()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddOrionStream());
    }

    [Fact]
    public void AddOrionStream_returns_the_same_collection_for_chaining()
    {
        var services = new ServiceCollection();

        var result = services.AddOrionStream();

        Assert.Same(services, result);
    }

    [Fact]
    public void AddOrionStream_registers_options_diagnostics_and_hub()
    {
        var services = new ServiceCollection();
        services.AddOrionStream();

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<StreamOptions>());
        Assert.NotNull(provider.GetService<StreamDiagnostics>());
        Assert.NotNull(provider.GetService<ISseHub>());
    }

    [Fact]
    public void The_hub_options_and_diagnostics_are_singletons()
    {
        var services = new ServiceCollection();
        services.AddOrionStream();

        using var provider = services.BuildServiceProvider();

        Assert.Same(provider.GetRequiredService<ISseHub>(), provider.GetRequiredService<ISseHub>());
        Assert.Same(provider.GetRequiredService<StreamOptions>(), provider.GetRequiredService<StreamOptions>());
        Assert.Same(
            provider.GetRequiredService<StreamDiagnostics>(),
            provider.GetRequiredService<StreamDiagnostics>());
    }

    [Fact]
    public void Default_options_are_used_when_no_configuration_is_supplied()
    {
        var services = new ServiceCollection();
        services.AddOrionStream();

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<StreamOptions>();

        Assert.Equal(256, options.SubscriberCapacity);
        Assert.Equal(TimeSpan.FromSeconds(15), options.HeartbeatInterval);
    }

    [Fact]
    public void Configuration_can_set_both_options()
    {
        var services = new ServiceCollection();
        services.AddOrionStream(o =>
        {
            o.SubscriberCapacity = 64;
            o.HeartbeatInterval = TimeSpan.FromSeconds(5);
        });

        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<StreamOptions>();

        Assert.Equal(64, options.SubscriberCapacity);
        Assert.Equal(TimeSpan.FromSeconds(5), options.HeartbeatInterval);
    }

    [Fact]
    public void Invalid_heartbeat_is_rejected_eagerly()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(
            () => services.AddOrionStream(o => o.HeartbeatInterval = TimeSpan.Zero));
    }

    [Fact]
    public void A_pre_existing_hub_registration_is_not_overwritten()
    {
        // TryAddSingleton semantics: an existing ISseHub registration wins.
        var services = new ServiceCollection();
        var diag = new StreamDiagnostics();
        var custom = new SseHub(new StreamOptions(), diag);
        services.AddSingleton<ISseHub>(custom);

        services.AddOrionStream();

        using var provider = services.BuildServiceProvider();
        Assert.Same(custom, provider.GetRequiredService<ISseHub>());
        diag.Dispose();
    }

    [Fact]
    public void The_registered_hub_actually_broadcasts()
    {
        var services = new ServiceCollection();
        services.AddOrionStream(o => o.SubscriberCapacity = 4);

        using var provider = services.BuildServiceProvider();
        var hub = provider.GetRequiredService<ISseHub>();
        using var sub = hub.Subscribe("orders");

        var delivered = hub.Publish("orders", new ServerSentEvent { Data = "hi" });

        Assert.Equal(1, delivered);
        Assert.True(sub.Reader.TryRead(out var evt));
        Assert.Equal("hi", evt!.Data);
    }
}
