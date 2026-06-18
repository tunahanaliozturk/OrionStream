namespace Moongazing.OrionStream.Tests;

using System;

using Moongazing.OrionStream;

using Xunit;

public sealed class StreamOptionsTests
{
    [Fact]
    public void Defaults_are_256_capacity_and_15_second_heartbeat()
    {
        var options = new StreamOptions();

        Assert.Equal(256, options.SubscriberCapacity);
        Assert.Equal(TimeSpan.FromSeconds(15), options.HeartbeatInterval);
        Assert.Equal(256, options.ReplayBufferCapacity);
    }

    [Fact]
    public void A_zero_replay_buffer_capacity_is_valid()
    {
        var options = new StreamOptions { ReplayBufferCapacity = 0 };

        options.Validate();
    }

    [Fact]
    public void A_negative_replay_buffer_capacity_is_rejected()
    {
        var options = new StreamOptions { ReplayBufferCapacity = -1 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.ReplayBufferCapacity), ex.ParamName);
    }

    [Fact]
    public void Default_options_validate_successfully()
    {
        var options = new StreamOptions();

        // Validate is internal; reachable via InternalsVisibleTo. No throw means valid.
        options.Validate();
    }

    [Fact]
    public void Capacity_of_one_is_valid()
    {
        var options = new StreamOptions { SubscriberCapacity = 1 };

        options.Validate();
    }

    [Fact]
    public void Zero_capacity_is_rejected()
    {
        var options = new StreamOptions { SubscriberCapacity = 0 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.SubscriberCapacity), ex.ParamName);
    }

    [Fact]
    public void Negative_capacity_is_rejected()
    {
        var options = new StreamOptions { SubscriberCapacity = -5 };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.SubscriberCapacity), ex.ParamName);
    }

    [Fact]
    public void Zero_heartbeat_interval_is_rejected()
    {
        var options = new StreamOptions { HeartbeatInterval = TimeSpan.Zero };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.HeartbeatInterval), ex.ParamName);
    }

    [Fact]
    public void Negative_heartbeat_interval_is_rejected()
    {
        var options = new StreamOptions { HeartbeatInterval = TimeSpan.FromSeconds(-1) };

        var ex = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(nameof(StreamOptions.HeartbeatInterval), ex.ParamName);
    }

    [Fact]
    public void A_one_tick_heartbeat_interval_is_valid()
    {
        var options = new StreamOptions { HeartbeatInterval = TimeSpan.FromTicks(1) };

        options.Validate();
    }
}
